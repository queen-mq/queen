// The little book of QueenMQ: the use cases of chapter 3 ("Real applications
// examples"), in C++.
//
// The book shows the regions between `guide:start(<id>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each use case does what the book
// says it does.
//
// The C++ client has no streams, so the trace merger (uc-trace) and the rate
// limiter (uc-rate) are not here. uc-order sends its transaction to the
// broker's HTTP API through the client's own HTTP layer, because a transaction
// built with client.transaction() cannot give its push a partition.
//
// Build it from this directory, as one command (01-hello-world.cpp says why
// each flag is there):
//   g++ -std=c++17 -O1 -pthread -I../../../clients/client-cpp
//       -I../../../clients/server/vendor -I/opt/homebrew/include
//       -I"$(brew --prefix openssl)/include" book-use-cases.cpp -o build/book-use-cases
//       -L"$(brew --prefix openssl)/lib" -lssl -lcrypto -lpthread
//
// Run it against a node:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
//   ./build/book-use-cases
// run.sh builds a copy with localhost:6632 replaced by $QUEEN_URL.

#include "queen_client.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

// A failed check travels to main() as an exception, which prints it and exits 1.
static void check(bool ok, const std::string& what) {
    if (!ok) throw std::runtime_error(what);
}

static long long millis_since(Clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count();
}

// What the work queue's workers did, from three threads.
static std::mutex done_mutex;
static std::vector<std::pair<std::string, std::string>> done;  // photo, worker
static void resize(const std::string& photo, const std::string& worker) {
    {
        std::lock_guard<std::mutex> hold(done_mutex);
        done.emplace_back(photo, worker);
    }
    std::this_thread::sleep_for(50ms);
}

// What each subscriber group saw, from several threads.
static std::mutex seen_mutex;
static std::map<std::string, std::vector<int>> seen;
static void note(const std::string& group, const json& order_id) {
    std::lock_guard<std::mutex> hold(seen_mutex);
    seen[group].push_back(order_id.get<int>());
}

int main() try {
    queen::QueenClient client("http://localhost:6632");
    for (const char* q : {"thumbnails", "order-placed", "chat", "order-events", "shipping",
                          "availability", "review-reminders", "webhooks", "daily-reports"}) {
        try {
            client.queue(q).del();
        } catch (const std::exception&) {
        }
    }
    client.kv("orders").del("order-2001");
    client.timers().cancel("review-reminders", "review-2001");
    client.ephemeral().reset("quotes");

    // --- Work queue: competing consumers ---------------------------------------
    // guide:start(uc-work)
    // Twelve photos to resize, from four users.
    for (int i = 1; i <= 12; i++) {
        client.queue("thumbnails").partition("user-" + std::to_string(i % 4))
            .push({{{"data", {{"photo", "p" + std::to_string(i) + ".jpg"}}}}});
    }

    // Three workers compete for the jobs: each job goes to exactly one of them.
    auto worker = [&](const std::string& name) {
        client.queue("thumbnails")
            .partitions(1)      // one user's photos at a time
            .each()
            .idle_millis(1500)  // stop when there is nothing left
            .wait(false)        // short polls: the idle clock is read between them
            .consume([&](const json& job) { resize(job["data"]["photo"], name); });
    };

    std::thread w1(worker, "w1"), w2(worker, "w2"), w3(worker, "w3");
    w1.join(); w2.join(); w3.join();
    // guide:end
    {
        std::set<std::string> photos, workers;
        std::map<std::string, int> per_worker;
        for (const auto& [photo, who] : done) {
            photos.insert(photo);
            workers.insert(who);
            per_worker[who]++;
        }
        check(done.size() == 12, "twelve jobs handled (" + std::to_string(done.size()) + ")");
        check(photos.size() == 12, "each job once");
        check(workers.size() >= 2, "the jobs were shared");
        std::cout << "ok: uc-work " << json(per_worker).dump() << std::endl;
    }

    // --- Publish/subscribe and fan-out: consumer groups ------------------------
    // guide:start(uc-pubsub)
    auto subscribe = [&](const std::string& group) {
        client.queue("order-placed")
            .group(group)                  // each group gets every message
            .subscription_mode("all")
            .each()
            .idle_millis(1000) // guide:hide
            .wait(false) // guide:hide
            .consume([&](const json& event) {
                note(group, event["data"]["orderId"]); // guide:hide
                std::cout << group + " " + event["data"]["orderId"].dump() + "\n";
            });
    };

    for (int order_id : {3001, 3002, 3003}) {
        client.queue("order-placed").partition("order-" + std::to_string(order_id))
            .push({{{"data", {{"orderId", order_id}}}}});
    }
    std::thread billing(subscribe, "billing"), shipping(subscribe, "shipping");
    billing.join(); shipping.join();

    // Months later, analytics subscribes too, and reads every order ever placed.
    subscribe("analytics");
    // guide:end
    {
        for (const char* group : {"billing", "shipping", "analytics"}) {
            std::vector<int> got = seen[group];
            std::sort(got.begin(), got.end());
            check(got == std::vector<int>{3001, 3002, 3003},
                  std::string(group) + " got every order: " + json(got).dump());
        }
        // A group that does not ask for 'all' starts at the end of the queue.
        json late = client.queue("order-placed").group("late").wait(false).batch(10).pop();
        check(late.empty(), "a new group without subscription_mode all skips the history");
        std::cout << "ok: uc-pubsub" << std::endl;
    }

    // --- 1. Chat processing: a burst is one batch (windowBuffer) ---------------
    Clock::time_point t0;
    int calls = 0;
    std::string question;
    // guide:start(uc-chat)
    // A burst waits 2 seconds, then goes to the handler as one batch.
    queen::QueueConfig chat;
    chat.window_buffer = 2;
    client.queue("chat").config(chat).create();
    t0 = Clock::now(); // guide:hide

    // The guest sends three messages in a row.
    for (const char* text : {"Hi!", "Is breakfast included?", "And is there parking?"}) {
        client.queue("chat").partition("conv-42").push({{{"data", {{"text", text}}}}});
    }

    // The assistant answers the whole burst once.
    client.queue("chat")
        .group("assistant")
        .subscription_mode("all")
        .batch(10)                         // the handler gets every message of the burst
        .limit(3) // guide:hide
        .consume([&](const json& messages) {
            calls++; // guide:hide
            question.clear();
            for (const json& m : messages) {
                if (!question.empty()) question += " ";
                question += m["data"]["text"].get<std::string>();
            }
            // Hi! Is breakfast included? And is there parking?
            std::cout << question << "\n";
        });
    // guide:end
    {
        long long waited = millis_since(t0);
        check(calls == 1, "the three messages reached the handler in one call");
        check(question == "Hi! Is breakfast included? And is there parking?", question);
        check(waited >= 1500, "the burst was held for the window (" + std::to_string(waited) +
                                  " ms)");
        std::cout << "ok: uc-chat (one call, after " << waited << " ms)" << std::endl;
    }

    // --- 2. Order workflow: transitions that cannot race (expect) --------------
    client.kv("orders").put("order-2001", {{"status", "paid"}}, queen::KvTtl::seconds(30 * 86400));
    client.queue("order-events").partition("order-2001")
        .push({{{"transactionId", "order-2001-paid"}, {"data", {{"orderId", "order-2001"}}}}});
    std::vector<bool> verdicts;
    // guide:start(uc-order)
    client.queue("order-events")
        .group("workflow")
        .subscription_mode("all")
        .auto_ack(false)
        .each()
        .limit(1) // guide:hide
        .consume([&](const json& msg) {
            const std::string order_id = msg["data"]["orderId"];
            json order = client.kv("orders").get(order_id);  // value: {"status":"paid"}
            if (order["value"]["status"] != "paid") {        // already moved: just ack
                client.ack(msg, true, {{"group", "workflow"}});
                return;
            }

            // Do the work of this step here: charge the card, book the courier, ...

            queen::KvWriteOptions from_what_we_read;         // only from what we read
            from_what_we_read.expect = order["version"].get<long long>();
            from_what_we_read.required = true;
            // client.transaction() cannot give its push a partition, so the step goes
            // to the broker's transaction route through the client's own HTTP layer.
            json step = {
                {"operations", json::array({
                    {{"type", "ack"}, {"status", "completed"},
                     {"consumerGroup", "workflow"}, {"leaseId", msg["leaseId"]},
                     {"transactionId", msg["transactionId"]},
                     {"partitionId", msg["partitionId"]}},
                    {{"type", "push"}, {"items", json::array({{
                        {"queue", "shipping"}, {"partition", order_id},
                        {"transactionId", "ship-" + order_id},
                        {"payload", {{"orderId", order_id}}},
                    }})}},
                })},
                {"requiredLeases", json::array({msg["leaseId"]})},
                {"kv", json::array({queen::wire::kv_write_op("put", "orders", order_id,
                    {{"status", "shipping"}}, queen::KvTtl::seconds(30 * 86400),
                    from_what_we_read)})},
            };
            json result = client.get_http_client()->post("/api/v1/transaction", step);
            bool committed = result.value("success", false);
            verdicts.push_back(committed); // guide:hide
            if (!committed) return;  // moved by someone else: nothing written
        });
    // guide:end
    {
        check(verdicts == std::vector<bool>{true}, "the transition committed once");
        json order = client.kv("orders").get("order-2001");
        check(order["value"]["status"] == "shipping", "the order is shipping: " + order.dump());
        // The same transition from the old version is refused, and writes nothing.
        queen::KvWriteOptions stale_version;
        stale_version.expect = order["version"].get<long long>() - 1;
        stale_version.required = true;
        json stale = client.get_http_client()->post("/api/v1/transaction", {
            {"operations", json::array({
                {{"type", "push"}, {"items", json::array({{
                    {"queue", "shipping"}, {"partition", "order-2001"},
                    {"transactionId", "ship-again"}, {"payload", json::object()}}})}},
            })},
            {"kv", json::array({queen::wire::kv_write_op("put", "orders", "order-2001",
                {{"status", "shipping"}}, queen::KvTtl::seconds(30 * 86400), stale_version)})},
        });
        check(stale.value("success", true) == false,
              "a transition from a stale version rolls back: " + stale.dump());
        auto read = client.queue("shipping").fetch({"order-2001"});
        check(read["order-2001"].records.size() == 1, "one shipment, not two");
        std::cout << "ok: uc-order" << std::endl;
    }

    // --- 5. Only the newest value (conflation) ----------------------------------
    // guide:start(uc-latest)
    // The availability of a hotel changes often; the screen needs only the last value.
    for (int rooms : {12, 11, 9}) {
        client.queue("availability").partition("hotel-7")
            .push({{{"data", {{"hotel", 7}, {"rooms", rooms}}}}});
    }

    json latest = client.queue("availability")
        .group("screen")
        .subscription_mode("all")
        .conflation()  // the newest message of each hotel, the rest skipped
        .wait(false)
        .pop()
        .at(0);
    std::cout << latest["data"] << "\n";   // {"hotel":7,"rooms":9}
    client.ack(latest, true, {{"group", "screen"}});
    // guide:end
    {
        check(latest["data"] == json{{"hotel", 7}, {"rooms", 9}}, "the newest availability");
        json again = client.queue("availability").group("screen").wait(false).pop();
        check(again.empty(), "the older values were skipped, not delivered later");
        std::cout << "ok: uc-latest" << std::endl;
    }

    // --- 6. Reminders and delayed jobs (timers) ---------------------------------
    std::vector<std::string> statuses;
    // guide:start(uc-reminder)
    // When an order is delivered, ask for a review in 24 hours.
    queen::TimerSchedule reminder;
    reminder.queue = "review-reminders";
    reminder.timer_key = "review-2001";
    reminder.payload = {{"orderId", 2001}};
    reminder.delay_millis = 24 * 60 * 60 * 1000;
    json first = client.timers().schedule(reminder);   // status: "scheduled"
    statuses.push_back(first["status"]); // guide:hide

    // The delivery was late: schedule the same key again, and the reminder moves.
    reminder.delay_millis = 48 * 60 * 60 * 1000;
    json moved = client.timers().schedule(reminder);   // status: "rescheduled"
    statuses.push_back(moved["status"]); // guide:hide

    // The guest wrote a review already: no reminder at all.
    client.timers().cancel("review-reminders", "review-2001");
    // guide:end
    {
        check(statuses == std::vector<std::string>{"scheduled", "rescheduled"},
              "scheduled, then rescheduled: " + json(statuses).dump());
        // The C++ client has no peek: GET the timer's route.
        json peek = client.get_http_client()->get("/api/v1/timers/review-reminders/review-2001");
        check(peek["found"] == false, "the cancelled reminder is gone: " + peek.dump());
        // A timer fires as a real message of its queue.
        queen::TimerSchedule soon;
        soon.queue = "review-reminders";
        soon.timer_key = "review-2002";
        soon.payload = {{"orderId", 2002}};
        soon.delay_millis = 200;
        client.timers().schedule(soon);
        json fired;
        client.queue("review-reminders").group("mailer").subscription_mode("all").each().limit(1)
            .wait(false).idle_millis(10000)
            .consume([&](const json& m) { fired = m["data"]; });
        check(fired == json{{"orderId", 2002}}, "the timer fired as a message: " + fired.dump());
        std::cout << "ok: uc-reminder" << std::endl;
    }

    // --- 7. Request and reply between services (ephemeral queues) --------------
    // guide:start(uc-reply)
    // The pricing service answers quote requests, in memory.
    auto pricing = [&] {
        queen::EphemeralPopOptions take;
        take.group = "pricing";
        take.wait = true;
        json batch = client.ephemeral().pop("quotes", take);
        for (const json& m : batch["messages"]) {
            json answer = {{"room", m["payload"]["room"]}, {"price", 129}};
            client.ephemeral().push(m["payload"]["replyTo"], json::array({answer}));
        }
        queen::EphemeralAckOptions done_with;
        done_with.group = "pricing";
        client.ephemeral().ack("quotes", batch["messages"], done_with);
    };

    // The booking page asks, and waits at most half a second for the answer.
    auto quote = [&](const std::string& room) -> json {
        auto inbox = "inbox-" + queen::util::generate_uuid_v7();
        json request = {{"room", room}, {"replyTo", inbox}};
        client.ephemeral().push("quotes", json::array({request}));
        queen::EphemeralPopOptions reply_wait;
        reply_wait.wait = true;
        reply_wait.timeout_millis = 500;
        reply_wait.commit_on_delivery = true;
        json reply = client.ephemeral().pop(inbox, reply_wait);
        if (reply["messages"].empty()) {
            return {{"room", room}, {"price", nullptr}};       // too slow: show "ask us"
        }
        return reply["messages"][0]["payload"];
    };

    auto answering = std::async(std::launch::async, pricing);
    json deluxe = quote("deluxe");
    answering.get();
    std::cout << deluxe << "\n";                       // {"price":129,"room":"deluxe"}
    // guide:end
    {
        check(deluxe == json{{"room", "deluxe"}, {"price", 129}}, "the quote: " + deluxe.dump());
        Clock::time_point t = Clock::now();
        json suite = quote("suite");                   // nobody answers now
        check(suite == json{{"room", "suite"}, {"price", nullptr}}, "no answer: " + suite.dump());
        check(millis_since(t) < 3000, "the caller gave up after its timeout");
        queen::EphemeralPopOptions drain;
        drain.group = "pricing";
        json left = client.ephemeral().pop("quotes", drain);
        queen::EphemeralAckOptions drained;
        drained.group = "pricing";
        client.ephemeral().ack("quotes", left["messages"], drained);
        std::cout << "ok: uc-reply" << std::endl;
    }

    // --- 8. Calling a webhook (retries, dead letters, idempotency key) ----------
    // A receiver that fails the first call, and keeps each idempotency key once.
    std::mutex hook_mutex;
    std::map<std::string, std::string> received;
    int calls_seen = 0;
    httplib::Server receiver;
    receiver.Post("/hooks", [&](const httplib::Request& req, httplib::Response& res) {
        std::lock_guard<std::mutex> hold(hook_mutex);
        if (++calls_seen == 1) {
            res.status = 500;
            return;
        }
        received[req.get_header_value("idempotency-key")] = req.body;
        res.status = 200;
    });
    const int hook_port = receiver.bind_to_any_port("127.0.0.1");
    std::thread receiver_thread([&] { receiver.listen_after_bind(); });
    receiver.wait_until_ready();
    const std::string hook_url = "http://127.0.0.1:" + std::to_string(hook_port) + "/hooks";
    client.get_http_client()->post("/api/v1/configure", {
        {"queue", "webhooks"}, {"options", {{"retryLimit", 5}, {"retryDelay", 1000}}}});
    client.queue("webhooks").partition("customer-42").push({{
        {"transactionId", "evt-9001"},
        {"data", {{"url", hook_url}, {"event", {{"type", "booking.confirmed"}, {"id", 9001}}}}},
    }});
    auto stop_when_received = [&] {
        for (;;) {
            {
                std::lock_guard<std::mutex> hold(hook_mutex);
                if (!received.empty()) break;
            }
            std::this_thread::sleep_for(50ms);
        }
        std::raise(SIGTERM);  // what a deploy sends
    };
    // guide:start(uc-webhook)
    static std::atomic<bool> stop{false};            // static: a signal handler sees it
    std::signal(SIGTERM, [](int) { stop = true; });  // a deploy stops the sender cleanly
    std::thread hook_watch(stop_when_received); // guide:hide

    client.queue("webhooks")
        .group("sender")
        .subscription_mode("all")
        .each()
        .wait(false)  // short polls: the stop flag is read between them
        .consume([](const json& msg) {
            const std::string url = msg["data"]["url"];       // http://host:port/path
            const size_t path = url.find('/', url.find("//") + 2);
            httplib::Client http(url.substr(0, path));
            httplib::Headers headers = {
                {"idempotency-key", msg["transactionId"].get<std::string>()}};
            auto res = http.Post(url.substr(path), headers, msg["data"]["event"].dump(),
                                 "application/json");
            if (!res || res->status / 100 != 2) {             // retried, then dead-lettered
                throw std::runtime_error("HTTP " + std::to_string(res ? res->status : 0));
            }
        }, &stop);
    // guide:end
    {
        hook_watch.join();
        receiver.stop();
        receiver_thread.join();
        check(calls_seen == 2, "the failed call was retried once (" +
                                   std::to_string(calls_seen) + " calls)");
        check(received.size() == 1 && received.count("evt-9001") == 1,
              "the receiver got the event under its key");
        check(json::parse(received["evt-9001"]) == json{{"type", "booking.confirmed"},
                                                        {"id", 9001}},
              "with its body");
        std::cout << "ok: uc-webhook" << std::endl;
    }

    // --- 9. A job only one worker runs (lock + guard) ---------------------------
    auto nightly_worker = [&]() -> std::string {
        // guide:start(uc-singleton)
        queen::Lock lock = client.lock("nightly-report", 30);
        if (!lock.acquire(1000)) return "skipped";  // another worker has it
        json result;
        try {
            json report = {{"day", "2026-10-24"}, {"bookings", 412}};
            std::string id = "report-" + report["day"].get<std::string>();
            result = client.transaction()
                .guard(lock)  // commits only while the lock is ours
                .queue("daily-reports").push({{{"transactionId", id}, {"data", report}}})
                .commit();
            std::this_thread::sleep_for(1500ms); // guide:hide
        } catch (...) {
            lock.release();
            throw;
        }
        lock.release();
        return result.value("success", false) ? "done" : "lost the lock";
        // guide:end
    };
    {
        auto a = std::async(std::launch::async, nightly_worker);
        auto b = std::async(std::launch::async, nightly_worker);
        auto c = std::async(std::launch::async, nightly_worker);
        std::vector<std::string> outcomes = {a.get(), b.get(), c.get()};
        check(std::count(outcomes.begin(), outcomes.end(), "done") == 1,
              "one worker ran it: " + json(outcomes).dump());
        auto read = client.queue("daily-reports").fetch({"Default"});
        check(read["Default"].records.size() == 1, "one report");
        std::cout << "ok: uc-singleton " << json(outcomes).dump() << std::endl;
    }

    client.close();
    std::cout << "\nPASS" << std::endl;
    return 0;
} catch (const std::exception& e) {
    std::cerr << "FAIL: " << e.what() << std::endl;
    return 1;
}
