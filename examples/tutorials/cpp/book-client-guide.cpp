// The little book of QueenMQ: the examples of chapter 2 ("Writing applications")
// that are not already in the five-minute guide (00-five-minutes.cpp), in C++.
//
// The book shows the regions between `guide:start(<id>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each example does what the book
// says it does.
//
// The C++ client has no streams, so the four stream examples are not here. It
// has no admin calls, no ack status other than completed and failed, and a
// QueueConfig without retryDelay, dedupWindowSeconds, retentionEnabled or
// maxWaitTimeSeconds: configure-jobs, ack-each-status, consume-callbacks (its
// dlq answer), seek and dlq-replay send those requests to the broker's HTTP API
// through the client's own HTTP layer, and say so.
//
// wire-forms needs the client's zstd support: build with -DQUEEN_CLIENT_ZSTD and
// link -lzstd. Without them the program still builds and runs, and says that it
// skipped wire-forms. From this directory, as one command:
//   g++ -std=c++17 -O1 -pthread -DQUEEN_CLIENT_ZSTD -I../../../clients/client-cpp
//       -I../../../clients/server/vendor -I/opt/homebrew/include
//       -I"$(brew --prefix openssl)/include" book-client-guide.cpp
//       -o build/book-client-guide -L/opt/homebrew/lib -L"$(brew --prefix openssl)/lib"
//       -lssl -lcrypto -lzstd -lpthread
//
// Run it against the node that the book's first section starts:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.6.0
//   ./build/book-client-guide
// run.sh builds a copy with localhost:6632 replaced by $QUEEN_URL.

#include "queen_client.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono_literals;

// The broker every client of this program talks to (run.sh rewrites it).
static const std::string BROKER = "http://localhost:6632";

// A failed check travels to main() as an exception, which prints it and exits 1.
static void check(bool ok, const std::string& what) {
    if (!ok) throw std::runtime_error(what);
}

// The helpers the examples call. In a real program they are yours.
struct PermanentError : std::runtime_error {
    using std::runtime_error::runtime_error;
};
static void send_email(const json&) {}
static std::atomic<int> ran{0};
static void run_task(const json&) {
    ++ran;
    std::this_thread::sleep_for(5ms);
}

#ifdef QUEEN_CLIENT_ZSTD
// Records what goes over the wire, to check the binary forms: a pass-through
// proxy in front of the broker that notes each request's and answer's type.
class WireRecorder {
public:
    struct Exchange {
        std::string target, sent_type, body, got_type;
    };

    explicit WireRecorder(const std::string& broker) : broker_(broker) {
        auto forward = [this](const httplib::Request& req, httplib::Response& res) {
            httplib::Client upstream(broker_);
            httplib::Result answer =
                req.method == "POST"
                    ? upstream.Post(req.target, req.body, req.get_header_value("Content-Type"))
                    : upstream.Get(req.target);
            if (!answer) {
                res.status = 502;
                return;
            }
            {
                std::lock_guard<std::mutex> hold(mutex_);
                exchanges_.push_back({req.target, req.get_header_value("Content-Type"), req.body,
                                      answer->get_header_value("Content-Type")});
            }
            res.status = answer->status;
            if (!answer->body.empty()) {
                res.set_content(answer->body, answer->get_header_value("Content-Type"));
            }
        };
        server_.Get(".*", forward);
        server_.Post(".*", forward);
        port_ = server_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { server_.listen_after_bind(); });
        server_.wait_until_ready();
    }

    ~WireRecorder() {
        server_.stop();
        thread_.join();
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    // The first exchange whose request target starts with `prefix`.
    Exchange find(const std::string& prefix) {
        std::lock_guard<std::mutex> hold(mutex_);
        for (const Exchange& e : exchanges_) {
            if (e.target.rfind(prefix, 0) == 0) return e;
        }
        return {};
    }

private:
    std::string broker_;
    httplib::Server server_;
    int port_ = 0;
    std::thread thread_;
    std::mutex mutex_;
    std::vector<Exchange> exchanges_;
};
#endif

// One request to the broker's ack route with any of the four statuses, for the
// checks below (QueenClient::ack sends completed or failed only).
static json ack_with(queen::QueenClient& client, const json& msg, const std::string& status,
                     const std::string& group, const std::string& error = "") {
    json body = {{"transactionId", msg["transactionId"]}, {"partitionId", msg["partitionId"]},
                 {"leaseId", msg["leaseId"]},             {"consumerGroup", group},
                 {"status", status}};
    if (!error.empty()) body["error"] = error;
    return client.get_http_client()->post("/api/v1/ack", body);
}

int main() try {
    queen::QueenClient client("http://localhost:6632");
    for (const char* q : {"jobs", "emails", "events", "reviews", "prices", "metrics", "tasks",
                          "payments", "invoices"}) {
        try {
            client.queue(q).del();
        } catch (const std::exception&) {
        }
    }

    // --- Configure ------------------------------------------------------------
    // guide:start(configure-jobs)
    // QueueConfig has none of retryDelay, dedupWindowSeconds, retentionEnabled and
    // maxWaitTimeSeconds, so the options go to the broker's configure route
    // through the client's own HTTP layer.
    json options = {
        {"leaseTime", 120},                  // 2 minutes per job, then another worker
        {"retryLimit", 5},                   // after 5 failures, the dead-letter queue
        {"retryDelay", 10000},               // a failed job returns after 10 s, not at once
        {"delayedProcessing", 30},           // a new job waits 30 s before anyone takes it
        {"windowBuffer", 5},                 // a burst waits 5 s, then goes out at once
        {"dedupWindowSeconds", 86400},       // a transactionId is remembered for a day
        {"retentionEnabled", true},          // retention runs only when this is on:
        {"completedRetentionSeconds", 3600}, //   jobs all groups finished go after an hour
        {"maxWaitTimeSeconds", 604800},      // older than a week goes, finished or not
    };
    client.get_http_client()->post("/api/v1/configure",
                                   {{"queue", "jobs"}, {"options", options}});
    // guide:end
    {
        json options = client.get_http_client()->get("/api/v1/resources/queues/jobs")["options"];
        check(options["leaseTime"] == 120, "leaseTime");
        check(options["retryDelay"] == 10000, "retryDelay");
        check(options["delayedProcessing"] == 30, "delayedProcessing");
        check(options["windowBuffer"] == 5, "windowBuffer");
        check(options["dedupWindowSeconds"] == 86400, "dedupWindowSeconds");
        check(options["retentionEnabled"] == true, "retentionEnabled");
        check(options["completedRetentionSeconds"] == 3600, "completedRetentionSeconds");
        check(options["maxWaitTimeSeconds"] == 604800, "maxWaitTimeSeconds");
        std::cout << "ok: configure-jobs" << std::endl;
    }

    // --- Push -----------------------------------------------------------------
    // guide:start(push-callbacks)
    json answers = client.queue("emails")
        .partition("customer-42")
        .push({{{"transactionId", "welcome-42"}, {"data", {{"to", "ada@example.com"}}}}});
    for (const json& answer : answers) {        // one answer per message, in order
        const std::string status = answer["status"];
        if (status == "queued") {
            std::cout << "stored at offset " << answer["offset"] << "\n";
        } else if (status == "duplicate") {
            std::cout << "already stored " << answer["transaction_id"] << "\n";
        } else {                                // "error": refused, nothing written
            std::cerr << "refused message " << answer["index"] << "\n";
        }
    }
    // guide:end
    {
        check(answers.size() == 1 && answers[0]["status"] == "queued", "the first push stored it");
        json again = client.queue("emails").partition("customer-42")
            .push({{{"transactionId", "welcome-42"}, {"data", {{"to", "ada@example.com"}}}}});
        check(again.size() == 1 && again[0]["status"] == "duplicate" &&
                  again[0]["transaction_id"] == "welcome-42",
              "the second push of the same id answered duplicate");
        std::cout << "ok: push-callbacks" << std::endl;
    }

    // guide:start(push-buffer)
    queen::BufferOptions batches;
    batches.message_count = 100;                // 100 at a time,
    batches.time_millis = 1000;                 // or every second
    for (int i = 0; i < 1000; i++) {
        client.queue("events")
            .partition("device-" + std::to_string(i % 10))
            .buffer(batches)
            .push({{{"transactionId", "reading-" + std::to_string(i)},
                    {"data", {{"device", i % 10}, {"value", i}}}}});
    }
    client.flush_all_buffers();                 // before exiting, send what is left
    // guide:end
    {
        std::vector<std::string> names;
        for (int i = 0; i < 10; i++) names.push_back("device-" + std::to_string(i));
        size_t total = 0;
        for (const auto& [name, partition] : client.queue("events").fetch(names)) {
            total += partition.records.size();
        }
        check(total == 1000, "every buffered reading was stored (" + std::to_string(total) + ")");
        std::cout << "ok: push-buffer" << std::endl;
    }

#ifdef QUEEN_CLIENT_ZSTD
    json wire_data;
    // guide:start(wire-forms)
    queen::ClientConfig config;
    config.binary_push = true;       // a push travels as one binary body, not JSON
    config.push_compression = true;  // with binary_push: each partition's batch is
                                     // zstd-compressed here
    config.framed_pop = true;        // a pop gets the stored records, and unpacks them here
    queen::QueenClient fast({"http://localhost:6632"}, config);

    fast.queue("invoices").partition("customer-42")
        .push({{{"transactionId", "invoice-2001"},
                {"data", {{"orderId", 2001}, {"total", 12.5}}}}});

    fast.queue("invoices").each().limit(1).consume([&](const json& msg) {
        wire_data = msg["data"]; // guide:hide
        std::cout << msg["data"] << "\n";  // {"orderId":2001,"total":12.5}, as with JSON
    });
    // guide:end
    {
        check(wire_data == json{{"orderId", 2001}, {"total", 12.5}}, "the consumer read the invoice");
        // A client with the same config, through a recorder: what does it send and get?
        WireRecorder recorder(BROKER);
        queen::QueenClient probe({recorder.url()}, config);
        probe.queue("invoices").partition("customer-42")
            .push({{{"transactionId", "invoice-2002"}, {"data", {{"orderId", 2002}, {"total", 7}}}}});
        json popped = probe.queue("invoices").batch(1).wait(false).pop();
        check(popped.size() == 1 && popped[0]["data"]["orderId"] == 2002, "the probe read it back");
        auto push = recorder.find("/api/v1/push");
        auto pop = recorder.find("/api/v1/pop/");
        check(push.sent_type == "application/x-queen-push", "the push went as a binary body");
        const std::string zstd_magic = "\x28\xb5\x2f\xfd";
        check(push.body.find(zstd_magic) != std::string::npos, "the binary push carried a zstd frame");
        check(pop.target.find("frames=true") != std::string::npos, "the pop asked for frames");
        check(pop.got_type == "application/x-queen-pop", "the broker answered the pop with frames");
        probe.close();
        fast.close();
        std::cout << "ok: wire-forms" << std::endl;
    }
#else
    std::cout << "skip: wire-forms (build with -DQUEEN_CLIENT_ZSTD and -lzstd)" << std::endl;
#endif

    // --- Consume --------------------------------------------------------------
    size_t archived = 0;
    // guide:start(consume-batch)
    client.queue("events")
        .group("archiver")
        .subscription_mode("all")
        .batch(100)                          // up to 100 messages per call
        .limit(1000)
        .consume([&](const json& messages) { // no each(): the handler gets the whole batch
            std::cout << "archiving " << messages.size() << "\n";
            archived += messages.size(); // guide:hide
        });                                  // all acked if it returns, nacked if it throws
    // guide:end
    check(archived == 1000, "the batch handler saw every reading once (" +
                                std::to_string(archived) + ")");
    std::cout << "ok: consume-batch" << std::endl;

    // guide:start(pop-ack)
    json messages = client.queue("events")
        .group("auditor")
        .subscription_mode("all")
        .batch(50)
        .wait(true)                    // long poll: wait for messages, never return empty
        .pop();
    std::cout << "auditing " << messages.size() << "\n";
    client.ack(messages, true, {{"group", "auditor"}});  // the whole batch at once
    // guide:end
    check(!messages.empty() && messages.size() <= 50, "pop returned a batch");
    std::cout << "ok: pop-ack" << std::endl;

    client.queue("reviews").partition("shop-3").push({
        {{"transactionId", "review-1"}, {"data", {{"stars", 5}}}},
        {{"transactionId", "review-2"}, {"data", {{"stars", 1}}}},
    });
    json reviews = client.queue("reviews").partition("shop-3").group("moderation")
        .subscription_mode("all").batch(2).wait(true).pop();
    check(reviews.size() == 2, "both reviews were popped");
    const json first = reviews[0];
    const json second = reviews[1];
    // guide:start(ack-each-status)
    // QueenClient::ack gives a whole batch one answer, so a batch with an answer per
    // message goes to the broker's batch ack route through the client's HTTP layer.
    client.get_http_client()->post("/api/v1/ack/batch", {
        {"consumerGroup", "moderation"},
        {"acknowledgments", json::array({
            {{"transactionId", first["transactionId"]}, {"leaseId", first["leaseId"]},
             {"partitionId", first["partitionId"]}, {"status", "completed"}},
            {{"transactionId", second["transactionId"]}, {"leaseId", second["leaseId"]},
             {"partitionId", second["partitionId"]}, {"status", "failed"},
             {"error", "timeout"}},
        })},
    });
    // guide:end
    {
        // The failed review comes back after the queue's retryDelay (1 s by default).
        json back;
        for (int i = 0; i < 25 && back.empty(); i++) {
            std::this_thread::sleep_for(200ms);
            back = client.queue("reviews").partition("shop-3").group("moderation")
                .batch(2).wait(false).pop();
        }
        check(back.size() == 1 && back[0]["transactionId"] == "review-2",
              "only the failed review came back: " + back.dump());
        client.ack(back, true, {{"group", "moderation"}});
        std::cout << "ok: ack-each-status" << std::endl;
    }

    // guide:start(consume-callbacks)
    client.queue("emails")
        .group("mailer")
        .subscription_mode("all")
        .auto_ack(false)                        // the handler settles each message itself
        .each()
        .limit(1)
        .consume([&](const json& msg) {
            try {
                send_email(msg["data"]);
                client.ack(msg, true, {{"group", "mailer"}});
            } catch (const PermanentError& e) { // trying again cannot help
                // QueenClient::ack sends completed or failed only, so the dlq answer
                // goes to the broker's ack route through the client's HTTP layer.
                client.get_http_client()->post("/api/v1/ack", {
                    {"transactionId", msg["transactionId"]}, {"leaseId", msg["leaseId"]},
                    {"partitionId", msg["partitionId"]}, {"consumerGroup", "mailer"},
                    {"status", "dlq"}, {"error", e.what()}});
            } catch (const std::exception& e) { // may work later: spend a retry
                client.ack(msg, false, {{"group", "mailer"}, {"error", e.what()}});
            }
        });
    // guide:end
    check(client.queue("emails").group("mailer").wait(false).pop().empty(),
          "the handler acked the email");
    std::cout << "ok: consume-callbacks" << std::endl;

    for (int price : {10, 11, 12}) {
        client.queue("prices").partition("sku-A-1").push({{
            {"transactionId", "price-A-1-" + std::to_string(price)},
            {"data", {{"sku", "A-1"}, {"price", price}}},
        }});
    }
    // guide:start(conflation)
    json latest = client.queue("prices")
        .group("repricer")
        .subscription_mode("all")
        .conflation()                    // only the newest message of each partition
        .wait(false)
        .pop()
        .at(0);
    std::cout << latest["data"] << "\n"; // {"price":12,"sku":"A-1"}: 10 and 11 are skipped
    client.ack(latest, true, {{"group", "repricer"}});
    // guide:end
    check(latest["data"]["price"] == 12, "conflation delivered the newest price");
    check(client.queue("prices").group("repricer").conflation().wait(false).pop().empty(),
          "and moved past the older ones");
    std::cout << "ok: conflation" << std::endl;

    for (int i = 0; i < 5; i++) {
        client.queue("metrics").partition("cpu").push({{
            {"transactionId", "cpu-" + std::to_string(i)}, {"data", {{"load", i}}},
        }});
    }
    // guide:start(commit-on-delivery)
    json readings = client.queue("metrics")
        .group("live-chart")
        .subscription_mode("all")
        .commit_on_delivery()  // the cursor moves as they are handed out: no ack
        .batch(100)
        .wait(false)
        .pop();
    // guide:end
    check(readings.size() == 5, "commitOnDelivery handed out every reading");
    check(client.queue("metrics").group("live-chart").wait(false).pop().empty(),
          "and nothing is left to ack or redeliver");
    std::cout << "ok: commit-on-delivery" << std::endl;

    // guide:start(seek)
    // The C++ client has no admin calls: a seek is a POST to the broker's
    // consumer-group route, through the client's own HTTP layer.
    auto http = client.get_http_client();
    http->post("/api/v1/consumer-groups/archiver/queues/events/seek",
               {{"timestamp", "2026-10-22T00:00:00Z"}});
    // the archiver group now reads again everything pushed since that time

    http->post("/api/v1/consumer-groups/archiver/queues/events/seek", {{"toEnd", true}});
    // and now it skips everything it has not read yet
    // guide:end
    check(client.queue("events").group("archiver").wait(false).pop().empty(),
          "after a seek to the end the group has nothing left");
    std::cout << "ok: seek" << std::endl;

    for (int i = 0; i < 40; i++) {
        client.queue("tasks").partition("account-" + std::to_string(i % 8))
            .push({{{"transactionId", "task-" + std::to_string(i)}, {"data", {{"n", i}}}}});
    }
    auto stop_when_done = [] {
        while (ran < 40) std::this_thread::sleep_for(20ms);
        std::raise(SIGTERM);  // what a deploy sends
    };
    // guide:start(consume-run)
    static std::atomic<bool> stop{false};            // static: a signal handler sees it
    std::signal(SIGTERM, [](int) { stop = true; });  // a deploy stops the worker cleanly
    std::thread watcher(stop_when_done); // guide:hide

    client.queue("tasks")
        .group("workers")
        .subscription_mode("all")
        .concurrency(4)              // four handlers at once, on four partitions
        .renew_lease(true, 10000)    // long tasks keep their lease alive
        .idle_millis(60000)          // or stop after a minute with nothing to do
        .wait(false)                 // short polls: stop and idle are read between them
        .each()
        .consume([](const json& task) { run_task(task["data"]); }, &stop);
    // guide:end
    watcher.join();
    check(ran == 40, "four workers ran every task, then the signal stopped them (" +
                         std::to_string(ran) + ")");
    std::cout << "ok: consume-run" << std::endl;

    // --- Retries and the dead-letter queue --------------------------------------
    client.get_http_client()->post("/api/v1/configure", {
        {"queue", "payments"}, {"options", {{"retryLimit", 1}, {"retryDelay", 100}}}});
    client.queue("payments").partition("order-7")
        .push({{{"transactionId", "charge-7"}, {"data", {{"amount", 25}}}}});
    client.queue("payments").group("charger").subscription_mode("all").each().limit(2)
        .idle_millis(5000).wait(false)
        .consume([](const json&) { throw std::runtime_error("card declined"); });
    std::this_thread::sleep_for(300ms);
    // guide:start(dlq-read)
    json dead = client.queue("payments").dlq("charger").limit(20).get();
    for (const json& letter : dead["messages"]) {
        std::cout << letter["transactionId"] << " " << letter["errorMessage"] << " "
                  << letter["retryCount"] << "\n";
    }
    // guide:end
    check(dead["messages"].size() == 1, "the failing payment is in the dead-letter queue");
    check(dead["messages"][0]["errorMessage"] == "card declined", "with the handler's error");
    std::cout << "ok: dlq-read" << std::endl;

    // guide:start(dlq-replay)
    for (const json& letter : dead["messages"]) {
        // back into its partition, and out of the dead-letter queue: the broker's
        // replay route, through the client's own HTTP layer (no call for it in C++)
        const std::string id = letter["id"];
        client.get_http_client()->post("/api/v1/dlq/" + id + "/replay");
    }
    // guide:end
    {
        json again;
        for (int i = 0; i < 30 && again.empty(); i++) {
            again = client.queue("payments").group("charger").batch(10).wait(false).pop();
            if (again.empty()) std::this_thread::sleep_for(100ms);
        }
        check(again.size() == 1, "the replayed payment is delivered again");
        client.ack(again, true, {{"group", "charger"}});
        json left = client.queue("payments").dlq("charger").get();
        check(left["messages"].empty(), "and it left the dead-letter queue");
        std::cout << "ok: dlq-replay" << std::endl;
    }

    {
        // The four ack statuses the book's table describes, checked here. 'retry'
        // gives a message back at once without spending a retry; 'dlq' files it now.
        client.queue("payments").partition("order-8")
            .push({{{"transactionId", "charge-8"}, {"data", {{"amount", 9}}}}});
        client.queue("payments").partition("order-9")
            .push({{{"transactionId", "charge-9"}, {"data", {{"amount", 4}}}}});
        json r1 = client.queue("payments").partition("order-8").group("statuses")
            .subscription_mode("all").batch(1).wait(true).pop().at(0);
        check(ack_with(client, r1, "retry", "statuses").at(0).value("success", false),
              "the retry ack was taken");
        json r2 = client.queue("payments").partition("order-8").group("statuses")
            .batch(1).wait(true).pop().at(0);
        check(r2["transactionId"] == r1["transactionId"], "'retry' redelivered the same message");
        check(r2.value("retryCount", 0) == r1.value("retryCount", 0), "'retry' spent no retry");
        ack_with(client, r2, "dlq", "statuses", "bad payload");
        json filed = client.queue("payments").dlq("statuses").get();
        check(filed["messages"].size() == 1, "'dlq' filed the message in the dead-letter queue");
        check(filed["messages"][0]["errorMessage"] == "bad payload", "with its reason");
        std::cout << "ok: ack statuses" << std::endl;
    }

    // --- KV -------------------------------------------------------------------
    client.kv("carts").del("customer-42");
    client.kv("limits").del("api:customer-42");
    // guide:start(kv)
    // Every write names its lifetime: a ttl, or forever.
    client.kv("carts").put("customer-42", {{"items", 3}}, queen::KvTtl::seconds(7 * 86400));
    json cart = client.kv("carts").get("customer-42");   // {found, value, version, ...}

    // Write only if nobody wrote since we read it: the version is the condition.
    queen::KvWriteOptions unchanged;
    unchanged.expect = cart["version"].get<long long>();
    json saved = client.kv("carts").put("customer-42", {{"items", 4}},
                                        queen::KvTtl::seconds(7 * 86400), unchanged);
    std::cout << saved["applied"] << "\n";               // false if the version moved

    // A counter with a ceiling: past max, the increment does not apply.
    queen::KvIncrOptions ceiling;
    ceiling.max = 100;
    json hit = client.kv("limits").incr("api:customer-42", 1,
                                        queen::KvTtl::seconds(60), ceiling);
    if (!hit["applied"].get<bool>()) std::cout << "over the limit\n";  // reason: "limit"
    // guide:end
    {
        check(cart["found"] == true, "the cart was read back");
        check(cart["value"] == json({{"items", 3}}), "the cart: " + cart["value"].dump());
        check(saved["applied"] == true, "the conditional write applied: " + saved.dump());
        json stale = client.kv("carts").put("customer-42", {{"items", 9}},
                                            queen::KvTtl::seconds(7 * 86400), unchanged);
        check(stale["applied"] == false, "a write from the old version is refused");
        check(stale.value("reason", "") == "version", "reason: " + stale.dump());
        check(stale["value"] == json({{"items", 4}}), "and answers the current value: " + stale.dump());
        check(hit["applied"] == true, "the first increment applied: " + hit.dump());
        json capped = client.kv("limits").incr("api:customer-42", 100, queen::KvTtl::seconds(60),
                                               ceiling);
        check(capped["applied"] == false, "an increment past max does not apply");
        check(capped.value("reason", "") == "limit", "reason: " + capped.dump());
        check(queen::kv_int64(capped["value"]) == 1,
              "and the counter keeps its value: " + capped.dump());
        std::cout << "ok: kv" << std::endl;
    }

    client.close();
    std::cout << "\nPASS" << std::endl;
    return 0;
} catch (const std::exception& e) {
    std::cerr << "FAIL: " << e.what() << std::endl;
    return 1;
}
