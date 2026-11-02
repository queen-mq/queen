/**
 * Queen C++ Client - consume path contract test suite: the consume loop, and
 * the ack() and renew() calls it settles with
 *
 * WHAT THIS FILE IS FOR.
 *
 *   - ack() AND renew() READ THE BODY. Both routes answer HTTP 200 when the
 *     broker settled or extended nothing: the per-item `success` of the ack
 *     array, and the `success`/`renewed` of the renewal, are the only signal.
 *     A refusal is a value (AckResult / RenewResult with success false); a
 *     call that could not be made -- a 5xx, an answer that does not line up
 *     with the request -- throws. The consumer group and the lease an ack
 *     sends are the Message's own.
 *   - pop() THROWS ON FAILURE. An empty vector means an empty queue and
 *     nothing else: before 2.7.1 a 403 or a 5xx was logged and returned as [],
 *     which looked exactly like an idle queue.
 *   - A 4xx ON A POP STOPS consume() WITH THAT ERROR; A 5xx OR A NETWORK
 *     FAULT DOES NOT. A worker is a thread of its own, so an error thrown there
 *     is gone unless the loop carries it out: it is rethrown once every worker
 *     has stopped. A 5xx is a broker restarting or electing: the loop waits a
 *     second and polls again.
 *   - THE LOOP SAYS WHY IT STOPPED. limit() counts across ALL workers,
 *     concurrency(n) runs n threads at once, a Cancel stops the loop after the
 *     message it is on, and the ConsumeSummary names the reason.
 *   - renew_lease() RENEWS. While the handler runs, one
 *     POST /api/v1/lease/:leaseId/extend per lease per interval: every message
 *     of a pop shares its leaseId, so a batch is one request, not one per
 *     message. Nothing after the handler returns.
 *   - A HANDLER THAT THROWS IS NACKED, UNLESS auto_ack(false). With auto_ack
 *     (the default) a handler that returns is acked, and one that throws --
 *     anything -- is nacked with its what(). auto_ack(false) hands settling to
 *     the handler, failures included: nothing is sent, and the loop abandons
 *     the rest of the batch, since the handler's ack of a later message could
 *     commit the failed one. Left leased, the failed message comes back when
 *     its lease expires, which never charges the queue's retry budget
 *     (server/src/rsm/consume/pop.rs: the budget "is charged only by an
 *     explicit `failed`, never by an expiry"): a handler that wants the DLQ
 *     path nacks by itself.
 *   - A NACK DROPS THE LATER MESSAGES OF ITS PARTITION, AND ONLY THOSE. The
 *     nack releases the lease of its message's partition and leaves that
 *     partition's cursor at the completed prefix (server/src/rsm/consume/
 *     ack.rs, the `Failed` branch), so the later messages of that partition
 *     are redelivered and an ack for one of them is refused: handling them now
 *     only makes duplicates. The other partitions of a multi-partition pop
 *     keep their lease, and their messages are handled.
 *   - commit_on_delivery() IS A pop() OPTION. It puts the broker's
 *     `autoAck=true` on the pop and nothing else: the broker moves the group's
 *     cursor as it hands the messages out, so there is no lease and nothing to
 *     ack. consume() always leases its messages, so it refuses the option
 *     before any request, and auto_ack() never sends it.
 *
 * Like its siblings this runs against an in-process httplib::Server -- no
 * broker, so every response is the one the test chose:
 *
 *   make consumer && ./bin/test_consumer
 *
 * The end-to-end half, against a real broker, is the settlement section of
 * test_client.cpp.
 *
 * Siblings: test_conflation.cpp, test_autopilot.cpp, test_retry429.cpp.
 */

#include "queen_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <future>
#include <vector>
#include <string>
#include <set>
#include <map>
#include <algorithm>

using namespace queen;
using json = nlohmann::json;

#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define RESET "\033[0m"

// ============================================================================
// Test double: records every request and answers from a responder
// ============================================================================

struct RecordedCall {
    std::string method;
    std::string path;                        // path only, no query string
    std::string body;
    std::string target;                      // raw request target, query string included
    std::chrono::steady_clock::time_point at;  // when the stub received it
};

using Responder = std::function<void(const httplib::Request&, httplib::Response&)>;

class StubServer {
private:
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    Responder responder_;

    mutable std::mutex mutex_;
    std::vector<RecordedCall> calls_;

public:
    explicit StubServer(Responder responder)
        : responder_(std::move(responder)) {
        auto handler = [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                calls_.push_back({req.method, req.path, req.body, req.target,
                                  std::chrono::steady_clock::now()});
            }
            responder_(req, res);
        };

        server_.Get(".*", handler);
        server_.Post(".*", handler);
        server_.Put(".*", handler);
        server_.Delete(".*", handler);
        // The client keeps its connections alive, and an idle keep-alive
        // connection holds a server thread for up to five seconds: httplib's
        // default pool (cores - 1, at least 8) would queue a test that runs
        // more consume workers than that behind its own idle connections.
        server_.new_task_queue = [] { return new httplib::ThreadPool(64); };

        port_ = server_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this]() { server_.listen_after_bind(); });
        server_.wait_until_ready();
    }

    ~StubServer() {
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

    std::vector<RecordedCall> calls_with_prefix(const std::string& prefix) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<RecordedCall> out;
        for (const auto& call : calls_) {
            if (call.path.rfind(prefix, 0) == 0) out.push_back(call);
        }
        return out;
    }

    size_t count_with_prefix(const std::string& prefix) const {
        return calls_with_prefix(prefix).size();
    }
};

/// Cancels the consume loop after a budget, so a loop that fails to stop on
/// its own FAILS its test (its summary says Cancelled, fired() is true)
/// instead of hanging the suite (the idiom of test_conflation.cpp). Hand
/// token() to the builder: `.cancel(watchdog.token())`.
class StopAfter {
private:
    Cancel token_;
    std::atomic<bool> done_{false};
    std::atomic<bool> fired_{false};
    std::thread timer_;

public:
    explicit StopAfter(int budget_millis) {
        timer_ = std::thread([this, budget_millis]() {
            auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(budget_millis);
            while (!done_.load() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            if (!done_.load()) {
                fired_.store(true);
                token_.cancel();
            }
        });
    }

    ~StopAfter() {
        done_.store(true);
        if (timer_.joinable()) {
            timer_.join();
        }
    }

    const Cancel& token() const { return token_; }
    bool fired() const { return fired_.load(); }
};

long long millis_between(const RecordedCall& earlier, const RecordedCall& later) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(later.at - earlier.at).count();
}

// ============================================================================
// Broker shapes
// ============================================================================

/// A pop answer: `count` messages t1..tN, all under `lease`, the way a 2.x
/// broker sends a claim. Message i is in partitions[(i - 1) % size]: one
/// partition by default, several for a multi-partition claim.
std::string popped(int count, const std::string& lease, const std::string& group = "workers",
                   const std::vector<std::string>& partitions = {"p1"}) {
    json messages = json::array();
    for (int i = 1; i <= count; ++i) {
        messages.push_back({
            {"id", "t" + std::to_string(i)},
            {"transactionId", "t" + std::to_string(i)},
            {"partitionId", partitions[(i - 1) % partitions.size()]},
            {"partition", "Default"},
            {"leaseId", lease},
            {"consumerGroup", group},
            {"data", {{"n", i}}}
        });
    }
    return json{{"success", true}, {"leaseId", lease}, {"messages", messages},
                {"partitionsClaimed", 1}}.dump();
}

/// The ack wire answer, `[{index, transactionId, success, error, ...}]`, one
/// item per acknowledgment in request order, for the single and the batch
/// route alike. `refused` lists the transaction ids to answer success:false.
std::string ack_answer(const std::string& request_body,
                       const std::set<std::string>& refused = {}) {
    json body = json::parse(request_body);
    json items = body.contains("acknowledgments") ? body["acknowledgments"] : json::array({body});
    json out = json::array();
    int index = 0;
    for (const auto& item : items) {
        std::string txn = item.value("transactionId", "");
        bool ok = refused.count(txn) == 0;
        out.push_back({
            {"index", index++},
            {"transactionId", txn},
            {"success", ok},
            {"error", ok ? json(nullptr) : json("invalid or expired lease")},
            {"leaseReleased", false},
            {"dlq", false},
            {"noop", false}
        });
    }
    return out.dump();
}

/// The lease renewal answer of a 2.x broker (server/src/rsm/facade/real.rs).
std::string renew_answer(const std::string& lease, bool renewed) {
    json expires = renewed ? json("2030-01-01T00:00:00.000Z") : json(nullptr);
    return json{{"leaseId", lease}, {"success", renewed}, {"renewed", renewed ? 1 : 0},
                {"newExpiresAt", expires}, {"expiresAt", expires},
                {"lease_expires_at", expires}}.dump();
}

bool starts_with(const std::string& text, const std::string& prefix) {
    return text.rfind(prefix, 0) == 0;
}

/// A broker with one claim to hand out: the first pop gets `first_pop`, every
/// later one a 204. Acks are accepted, renewals succeed.
Responder one_claim(const std::string& first_pop) {
    auto pops = std::make_shared<std::atomic<int>>(0);
    return [pops, first_pop](const httplib::Request& req, httplib::Response& res) {
        if (starts_with(req.path, "/api/v1/pop")) {
            if ((*pops)++ == 0) {
                res.status = 200;
                res.set_content(first_pop, "application/json");
            } else {
                res.status = 204;
            }
            return;
        }
        if (starts_with(req.path, "/api/v1/ack")) {
            res.status = 200;
            res.set_content(ack_answer(req.body), "application/json");
            return;
        }
        if (starts_with(req.path, "/api/v1/lease/")) {
            std::string lease = req.path.substr(std::string("/api/v1/lease/").size());
            lease = lease.substr(0, lease.find('/'));
            res.status = 200;
            res.set_content(renew_answer(lease, true), "application/json");
            return;
        }
        res.status = 404;
    };
}

/// A broker that never runs dry: every pop claims `per_pop` new messages
/// (m1, m2, ... across pops), each in a partition of its own, all under the
/// pop's own lease. Acks are accepted.
Responder endless(int per_pop) {
    auto pops = std::make_shared<std::atomic<int>>(0);
    auto next = std::make_shared<std::atomic<int>>(0);
    return [pops, next, per_pop](const httplib::Request& req, httplib::Response& res) {
        if (starts_with(req.path, "/api/v1/pop")) {
            const std::string lease = "lease-" + std::to_string(++(*pops));
            json messages = json::array();
            for (int i = 0; i < per_pop; ++i) {
                const std::string id = "m" + std::to_string(++(*next));
                messages.push_back({{"id", id}, {"transactionId", id}, {"partitionId", "p-" + id},
                                    {"partition", "Default"}, {"leaseId", lease},
                                    {"consumerGroup", "workers"}, {"data", {{"id", id}}}});
            }
            res.status = 200;
            res.set_content(json{{"success", true}, {"leaseId", lease}, {"messages", messages},
                                 {"partitionsClaimed", per_pop}}.dump(),
                            "application/json");
            return;
        }
        if (starts_with(req.path, "/api/v1/ack")) {
            res.status = 200;
            res.set_content(ack_answer(req.body), "application/json");
            return;
        }
        res.status = 404;
    };
}

/// Every acknowledgment the client sent, flattened across /api/v1/ack and
/// /api/v1/ack/batch, in the order they arrived. The consumer group of a batch
/// is copied onto each of its items.
std::vector<json> acks_sent(const StubServer& server) {
    std::vector<json> out;
    for (const auto& call : server.calls()) {
        if (call.path == "/api/v1/ack") {
            out.push_back(json::parse(call.body));
        } else if (call.path == "/api/v1/ack/batch") {
            json body = json::parse(call.body);
            for (auto item : body["acknowledgments"]) {
                item["consumerGroup"] = body["consumerGroup"];
                out.push_back(item);
            }
        }
    }
    return out;
}

/// "t1:completed", "t2:failed", ... for every acknowledgment sent.
std::vector<std::string> settled(const StubServer& server) {
    std::vector<std::string> out;
    for (const auto& ack : acks_sent(server)) {
        out.push_back(ack.value("transactionId", "") + ":" + ack.value("status", ""));
    }
    return out;
}

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

// Client with the 429 backoff shortened so no test in this file can sleep.
ClientConfig fast_config() {
    ClientConfig config;
    config.retry_429.base_millis = 1;
    config.retry_429.cap_millis = 2;
    return config;
}

const char* reason_name(StopReason reason) {
    switch (reason) {
    case StopReason::Ended: return "Ended";
    case StopReason::Limit: return "Limit";
    case StopReason::Idle: return "Idle";
    case StopReason::Cancelled: return "Cancelled";
    }
    return "?";
}

std::string text(const ConsumeSummary& s) {
    return json{{"processed", s.processed}, {"acked", s.acked}, {"nacked", s.nacked},
                {"stoppedBy", reason_name(s.stopped_by)}}.dump();
}

std::string text(const AckResult& r) {
    return json{{"index", r.index}, {"transactionId", r.transaction_id}, {"success", r.success},
                {"error", r.error}, {"leaseReleased", r.lease_released}, {"dlq", r.dlq},
                {"noop", r.noop}, {"conflated", r.conflated ? json(*r.conflated) : json(nullptr)}}
        .dump();
}

std::string text(const std::vector<AckResult>& results) {
    std::string out = "[";
    for (size_t i = 0; i < results.size(); ++i) out += (i ? "," : "") + text(results[i]);
    return out + "]";
}

std::string text(const RenewResult& r) {
    return json{{"leaseId", r.lease_id}, {"success", r.success},
                {"newExpiresAt", r.new_expires_at}, {"error", r.error}}.dump();
}

// ============================================================================
// ack() and renew() read the body
// ============================================================================

/// A delivered message as a pop hands it over: its consumer group and lease
/// are what an ack of it sends.
Message message(const std::string& txn, const std::string& lease = "lease-1",
                const std::string& group = "workers") {
    return Message::from_json({{"id", txn}, {"transactionId", txn}, {"partitionId", "p1"},
                               {"leaseId", lease}, {"consumerGroup", group}});
}

void test_ack_reports_a_refused_acknowledgment() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        res.status = 200;
        res.set_content(ack_answer(req.body, {"t1"}), "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    AckResult single = client.ack(message("t1"));
    check(!single.success, "a refused ack must report success:false, got " + text(single));
    check(single.error.find("invalid or expired lease") != std::string::npos,
          "the broker's reason must reach the caller, got " + text(single));

    // One verdict per acknowledgment: the first message was refused, the
    // second accepted, and the result says which.
    std::vector<AckResult> batch = client.ack_all({message("t1"), message("t2")});
    check(batch.size() == 2, "a batch of two must return two verdicts, got " + text(batch));
    if (batch.size() != 2) return;
    check(!batch[0].success && batch[0].transaction_id == "t1",
          "the refused item of a batch must report success:false, got " + text(batch));
    check(batch[0].error.find("invalid or expired lease") != std::string::npos,
          "the batch must name the refusal, got " + text(batch));
    check(batch[1].success && batch[1].transaction_id == "t2",
          "the accepted item of the same batch is a success, got " + text(batch));
}

void test_ack_reports_an_accepted_acknowledgment() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        res.status = 200;
        if (req.path == "/api/v1/ack") {
            // Every field of the item set away from its default, so a result
            // that was not read from the answer cannot pass.
            res.set_content(R"([{"index":0,"transactionId":"t1","success":true,"error":null,)"
                            R"("leaseReleased":true,"dlq":false,"noop":true,"conflated":4}])",
                            "application/json");
            return;
        }
        res.set_content(ack_answer(req.body), "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    AckResult single = client.ack(message("t1"));
    check(single.success, "an accepted ack is a success, got " + text(single));
    check(single.index == 0 && single.transaction_id == "t1" && single.error.empty() &&
          single.lease_released && !single.dlq && single.noop && single.conflated == 4LL,
          "the broker's item must be read field by field into the result, got " + text(single));

    std::vector<AckResult> batch = client.ack_all({message("t1"), message("t2")});
    check(batch.size() == 2 && batch[0].success && batch[1].success,
          "an accepted batch is a success, item by item, got " + text(batch));
    check(batch.size() == 2 && batch[0].transaction_id == "t1" && batch[1].transaction_id == "t2" &&
          batch[1].index == 1,
          "the verdicts come back in request order, got " + text(batch));
}

// The consumer group and the lease come from the message itself, so a
// forgotten group cannot ack the queue-mode cursor (which is how a message used
// to come back). The wire bodies are the ones every 2.x broker reads.
void test_ack_takes_the_group_and_the_lease_from_the_message() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        res.status = 200;
        res.set_content(ack_answer(req.body), "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    client.ack(message("t1", "lease-1", "workers"));
    client.nack(message("t2", "lease-2", "workers"), "boom");
    client.ack(message("t3", "", ""));       // queue mode, and a commit-on-delivery pop's lease
    client.ack_all({message("t4", "lease-4", "billing"), message("t5", "lease-4", "billing")});
    // Queue mode as a 2.x broker names it on the message: still null on the wire.
    client.ack(message("t8", "lease-8", util::QUEUE_MODE_GROUP));
    client.ack_all({message("t9", "lease-9", util::QUEUE_MODE_GROUP)});

    auto calls = server.calls();
    check(calls.size() == 6, "six acks, six requests, got " + std::to_string(calls.size()));
    if (calls.size() != 6) return;

    json single = json::parse(calls[0].body);
    check(calls[0].path == "/api/v1/ack" &&
          single == json{{"transactionId", "t1"}, {"partitionId", "p1"}, {"status", "completed"},
                         {"error", nullptr}, {"consumerGroup", "workers"}, {"leaseId", "lease-1"}},
          "an ack must send the message's group and lease, got " + calls[0].path + " " +
          calls[0].body);

    json nack = json::parse(calls[1].body);
    check(nack["status"] == "failed" && nack["error"] == "boom" &&
          nack["consumerGroup"] == "workers" && nack["leaseId"] == "lease-2",
          "a nack must send its reason with the message's group and lease, got " + calls[1].body);

    json bare = json::parse(calls[2].body);
    check(bare.contains("consumerGroup") && bare["consumerGroup"].is_null() &&
          !bare.contains("leaseId"),
          "a message with no group acks queue mode (null) and sends no lease, got " +
          calls[2].body);

    json batch = json::parse(calls[3].body);
    check(calls[3].path == "/api/v1/ack/batch" && batch["consumerGroup"] == "billing" &&
          batch["acknowledgments"].size() == 2 &&
          batch["acknowledgments"][0] == json{{"transactionId", "t4"}, {"partitionId", "p1"},
                                              {"status", "completed"}, {"error", nullptr},
                                              {"leaseId", "lease-4"}},
          "a batch ack must carry the messages' group once and each lease, got " +
          calls[3].path + " " + calls[3].body);

    json queue_mode = json::parse(calls[4].body);
    check(queue_mode.contains("consumerGroup") && queue_mode["consumerGroup"].is_null() &&
          queue_mode["leaseId"] == "lease-8",
          "a message of group __QUEUE_MODE__ acks queue mode as null, with its lease, got " +
          calls[4].body);
    json queue_mode_batch = json::parse(calls[5].body);
    check(calls[5].path == "/api/v1/ack/batch" && queue_mode_batch.contains("consumerGroup") &&
          queue_mode_batch["consumerGroup"].is_null(),
          "a batch of group __QUEUE_MODE__ acks queue mode as null, got " + calls[5].body);

    // The batch route carries ONE group: messages of two are refused before
    // anything is sent.
    std::string refusal;
    try {
        client.ack_all({message("t6", "lease-6", "workers"), message("t7", "lease-6", "billing")});
    } catch (const std::invalid_argument& e) {
        refusal = e.what();
    }
    check(!refusal.empty(), "a batch ack across two consumer groups must be refused");
    check(server.calls().size() == 6, "the refusal must come before any request, got " +
          std::to_string(server.calls().size() - 6) + " more request(s)");
}

// A refusal is a value; a call that could not be made is not. Before 2.7.1 both
// came back as {success:false}, and a broker that was down read like a lease
// that had expired.
void test_ack_and_renew_throw_when_the_call_could_not_be_made() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        if (req.path == "/api/v1/ack/batch") {
            // One verdict for two acknowledgments: misaligned.
            res.status = 200;
            res.set_content(R"([{"index":0,"transactionId":"t1","success":true}])",
                            "application/json");
            return;
        }
        res.status = 500;
        res.set_content(R"({"error":"broker exploded"})", "application/json");
    });
    ClientConfig config = fast_config();
    config.retry_attempts = 1;               // one try: the 500 is the answer
    QueenClient client({server.url()}, config);

    int ack_status = 0;
    try {
        client.ack(message("t1"));
    } catch (const HttpError& e) {
        ack_status = e.status_code();
    }
    check(ack_status == 500, "an ack answered 500 must throw HttpError(500), got status " +
          std::to_string(ack_status));

    int renew_status = 0;
    try {
        client.renew("lease-1");
    } catch (const HttpError& e) {
        renew_status = e.status_code();
    }
    check(renew_status == 500, "a renewal answered 500 must throw HttpError(500), got status " +
          std::to_string(renew_status));

    std::string misaligned;
    bool http = false;
    try {
        client.ack_all({message("t1"), message("t2")});
    } catch (const HttpError&) {
        http = true;
    } catch (const Error& e) {
        misaligned = e.what();
    }
    check(!http && misaligned.find("for 2 acknowledgments") != std::string::npos,
          "one verdict for two acknowledgments must throw queen::Error, not be read against "
          "the wrong message; got: " + (http ? std::string("HttpError") : misaligned));
}

void test_renew_reports_a_lease_the_broker_did_not_extend() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        bool known = req.path == "/api/v1/lease/live/extend";
        res.status = 200;
        res.set_content(renew_answer(known ? "live" : "gone", known), "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    RenewResult gone = client.renew("gone");
    check(!gone.success,
          "a lease the broker did not extend must report success:false, got " + text(gone));
    check(!gone.error.empty(), "a failed renewal must say why, got " + text(gone));
    check(gone.lease_id == "gone", "the result must name its lease, got " + text(gone));

    RenewResult live = client.renew("live");
    check(live.success, "an extended lease is a success, got " + text(live));
    check(live.new_expires_at == "2030-01-01T00:00:00.000Z",
          "the new expiry must be reported, got " + text(live));
}

void test_renew_of_a_batch_renews_each_lease_once() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        bool known = req.path == "/api/v1/lease/live/extend";
        res.status = 200;
        res.set_content(renew_answer(known ? "live" : "gone", known), "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    RenewResult one = client.renew(message("t1", "live"));
    check(one.success && one.lease_id == "live",
          "renew(message) renews the message's lease, got " + text(one));

    // t3 shares t1's lease, and t4 (a commit-on-delivery delivery) has none.
    std::vector<RenewResult> results = client.renew(std::vector<Message>{
        message("t1", "live"), message("t2", "gone"), message("t3", "live"), message("t4", "")});
    check(results.size() == 2, "one renewal per distinct lease, got " +
          std::to_string(results.size()));
    if (results.size() == 2) {
        check(results[0].lease_id == "live" && results[0].success &&
              results[1].lease_id == "gone" && !results[1].success,
              "the renewals in first-seen order, each with its verdict, got " + text(results[0]) +
              " " + text(results[1]));
    }

    std::vector<std::string> paths;
    for (const auto& call : server.calls()) paths.push_back(call.path);
    check(paths == std::vector<std::string>({"/api/v1/lease/live/extend",
                                             "/api/v1/lease/live/extend",
                                             "/api/v1/lease/gone/extend"}),
          "one request per lease, got " + json(paths).dump());
}

// ============================================================================
// pop() throws instead of returning nothing
// ============================================================================

void test_pop_throws_instead_of_returning_nothing() {
    ClientConfig config = fast_config();
    config.retry_attempts = 1;               // no transport retries: one request per pop

    {
        // A 403: revoked credentials, a suspended cluster. Terminal, sent once.
        StubServer server([](const httplib::Request&, httplib::Response& res) {
            res.status = 403;
            res.set_content(R"({"error":"cluster suspended","code":"cluster_suspended"})",
                            "application/json");
        });
        QueenClient client({server.url()}, config);
        int status = 0;
        std::string code;
        bool terminal = false;
        try {
            client.queue("pop-403").group("workers").wait(false).pop();
        } catch (const HttpError& e) {
            status = e.status_code();
            code = e.code();
            terminal = e.is_terminal_refusal();
        }
        check(status == 403 && code == "cluster_suspended" && terminal,
              "a 403 on pop() must throw HttpError(403, cluster_suspended), got status " +
              std::to_string(status) + " code '" + code + "'");
        check(server.count_with_prefix("/api/v1/pop") == 1, "a 403 is not retried, got " +
              std::to_string(server.count_with_prefix("/api/v1/pop")) + " pops");
    }
    {
        // A 5xx the transport's retries did not outlast.
        StubServer server([](const httplib::Request&, httplib::Response& res) {
            res.status = 503;
            res.set_content(R"({"error":"no leader yet"})", "application/json");
        });
        QueenClient client({server.url()}, config);
        int status = 0;
        try {
            client.queue("pop-503").group("workers").wait(false).pop();
        } catch (const HttpError& e) {
            status = e.status_code();
        }
        check(status == 503, "a 503 on pop() must throw HttpError(503), got status " +
              std::to_string(status));
    }
    {
        // A refusal inside a 200.
        StubServer server([](const httplib::Request&, httplib::Response& res) {
            res.status = 200;
            res.set_content(R"({"success":false,"error":"queue is being deleted","messages":[]})",
                            "application/json");
        });
        QueenClient client({server.url()}, config);
        std::string what;
        try {
            client.queue("pop-refused").group("workers").wait(false).pop();
        } catch (const Error& e) {
            what = e.what();
        }
        check(what.find("queue is being deleted") != std::string::npos,
              "a 200 with success:false must throw with the broker's reason, got: " + what);
    }
    {
        // Nobody listening.
        std::string url;
        {
            StubServer gone([](const httplib::Request&, httplib::Response& res) { res.status = 204; });
            url = gone.url();
        }
        QueenClient client({url}, config);
        bool network = false;
        try {
            client.queue("pop-down").group("workers").wait(false).pop();
        } catch (const NetworkError&) {
            network = true;
        }
        check(network, "pop() against a closed port must throw NetworkError");
    }
    {
        // And an empty queue is still an empty vector.
        StubServer server([](const httplib::Request&, httplib::Response& res) { res.status = 204; });
        QueenClient client({server.url()}, config);
        std::vector<Message> messages = client.queue("pop-empty").group("workers").wait(false).pop();
        check(messages.empty(), "a 204 is an empty queue: pop() returns no message");
    }
}

// ============================================================================
// A 4xx on a pop stops consume() with that error, a 5xx does not
// ============================================================================

void test_a_4xx_on_a_pop_stops_consume_with_that_error() {
    StubServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 400;
        res.set_content(R"({"success":false,"error":"bad pop","code":"bad_request"})",
                        "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    int status = 0;
    std::string code;
    StopAfter watchdog(3000);
    try {
        client.queue("consume-pop-error").group("workers").wait(false).cancel(watchdog.token())
              .consume_batch([](const std::vector<Message>&) {});
    } catch (const HttpError& e) {
        status = e.status_code();
        code = e.code();
    }

    check(status == 400, "consume() must rethrow the broker's 400, got status " +
          std::to_string(status));
    check(code == "bad_request", "the rethrown error is the broker's, got code '" + code + "'");
    check(server.count_with_prefix("/api/v1/pop") == 1,
          "a refused pop is not retried, got " +
          std::to_string(server.count_with_prefix("/api/v1/pop")) + " pops");
}

// A 403 is terminal for every worker, and consume() throws it only once they
// have all stopped.
void test_a_403_on_a_pop_stops_every_worker_with_that_error() {
    StubServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 403;
        res.set_content(R"({"error":"cluster suspended","code":"cluster_suspended"})",
                        "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    int status = 0;
    std::string code;
    StopAfter watchdog(3000);
    try {
        client.queue("consume-403").group("workers").concurrency(3).wait(false)
              .cancel(watchdog.token())
              .consume([](const Message&) {});
    } catch (const HttpError& e) {
        status = e.status_code();
        code = e.code();
    }

    check(status == 403 && code == "cluster_suspended",
          "consume() must rethrow the 403, got status " + std::to_string(status) + " code '" +
          code + "'");
    check(!watchdog.fired(), "consume() must stop on the 403, not on the watchdog");
    const size_t pops = server.count_with_prefix("/api/v1/pop");
    check(pops >= 1 && pops <= 3,
          "each of the 3 workers polls at most once: a 403 is not retried, got " +
          std::to_string(pops) + " pops");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    check(server.count_with_prefix("/api/v1/pop") == pops,
          "no worker may still be polling once consume() has thrown");
}

void test_a_5xx_keeps_consume_polling() {
    // A 503 first (a broker electing a leader), then the claim.
    auto pops = std::make_shared<std::atomic<int>>(0);
    Responder claim = one_claim(popped(1, "lease-1"));
    StubServer server([pops, claim](const httplib::Request& req, httplib::Response& res) {
        if (starts_with(req.path, "/api/v1/pop") && (*pops)++ == 0) {
            res.status = 503;
            res.set_content(R"({"error":"no leader yet"})", "application/json");
            return;
        }
        claim(req, res);
    });
    ClientConfig config = fast_config();
    config.retry_attempts = 1;               // the loop's retry, not HttpClient's
    QueenClient client({server.url()}, config);

    std::atomic<int> handled{0};
    bool threw = false;
    ConsumeSummary summary;
    StopAfter watchdog(5000);
    try {
        summary = client.queue("consume-5xx").group("workers").wait(false).limit(1)
                        .cancel(watchdog.token())
                        .consume_batch([&handled](const std::vector<Message>& batch) {
                            handled += static_cast<int>(batch.size());
                        });
    } catch (const std::exception&) {
        threw = true;
    }

    check(!threw, "a 5xx must not stop consume()");
    check(handled.load() == 1, "the claim after the 5xx must be handled, handled " +
          std::to_string(handled.load()));
    check(summary.stopped_by == StopReason::Limit && summary.processed == 1,
          "the loop must end on its limit, got " + text(summary));
    auto polls = server.calls_with_prefix("/api/v1/pop");
    check(polls.size() == 2, "the loop polls again after the 503, once, got " +
          std::to_string(polls.size()) + " pops");
    if (polls.size() == 2) {
        const long long gap = millis_between(polls[0], polls[1]);
        check(gap >= 900 && gap <= 2500,
              "the loop waits about a second before polling again, waited " +
              std::to_string(gap) + " ms");
    }
}

void test_a_network_fault_keeps_consume_polling() {
    // The first pop is answered too late (the client gives up after 200 ms),
    // then the claim.
    auto pops = std::make_shared<std::atomic<int>>(0);
    Responder claim = one_claim(popped(1, "lease-1"));
    StubServer server([pops, claim](const httplib::Request& req, httplib::Response& res) {
        if (starts_with(req.path, "/api/v1/pop") && (*pops)++ == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(700));
            res.status = 204;
            return;
        }
        claim(req, res);
    });
    ClientConfig config = fast_config();
    config.retry_attempts = 1;               // the loop's retry, not HttpClient's
    QueenClient client({server.url()}, config);

    std::atomic<int> handled{0};
    bool threw = false;
    ConsumeSummary summary;
    StopAfter watchdog(5000);
    try {
        summary = client.queue("consume-network").group("workers").wait(false).timeout_millis(200)
                        .limit(1).cancel(watchdog.token())
                        .consume([&handled](const Message&) { handled++; });
    } catch (const std::exception& e) {
        threw = true;
        check(false, std::string("a network fault must not stop consume(), it threw: ") + e.what());
    }

    check(!threw && handled.load() == 1, "the claim after the timeout must be handled, handled " +
          std::to_string(handled.load()));
    check(summary.stopped_by == StopReason::Limit, "the loop must end on its limit, got " +
          text(summary));
    auto polls = server.calls_with_prefix("/api/v1/pop");
    check(polls.size() == 2, "the loop polls again after the timeout, once, got " +
          std::to_string(polls.size()) + " pops");
    if (polls.size() == 2) {
        const long long gap = millis_between(polls[0], polls[1]);
        check(gap >= 900 && gap <= 3000,
              "the loop waits about a second before polling again, the second pop came " +
              std::to_string(gap) + " ms after the first");
    }
}

// ============================================================================
// renew_lease() renews while the handler runs
// ============================================================================

void test_renew_lease_renews_while_the_handler_runs() {
    StubServer server(one_claim(popped(3, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    StopAfter watchdog(5000);
    ConsumeSummary summary =
        client.queue("consume-renew").group("workers").renew_lease(true, 100)
              .wait(false).idle_millis(300).cancel(watchdog.token())
              .consume_batch([](const std::vector<Message>&) {
                  std::this_thread::sleep_for(std::chrono::milliseconds(450));
              });

    size_t renewals = server.count_with_prefix("/api/v1/lease/");
    check(server.count_with_prefix("/api/v1/lease/lease-1/extend") == renewals,
          "every renewal must be for the batch's lease");
    // 450 ms at a 100 ms interval is four ticks; one request per message would
    // be twelve. The bounds leave room for a slow scheduler.
    check(renewals >= 2, "expected the lease renewed while the handler ran, got " +
          std::to_string(renewals) + " renewal(s)");
    check(renewals <= 6, "expected one renewal per lease per interval, got " +
          std::to_string(renewals));
    check(summary.stopped_by == StopReason::Idle && summary.processed == 3 && summary.acked == 3,
          "the batch is acked and the loop ends idle, got " + text(summary));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    check(server.count_with_prefix("/api/v1/lease/") == renewals,
          "renewal must stop when the handler returns");
}

void test_no_renewal_unless_asked() {
    StubServer server(one_claim(popped(1, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    StopAfter watchdog(5000);
    ConsumeSummary summary =
        client.queue("consume-no-renew").group("workers")
              .wait(false).idle_millis(300).cancel(watchdog.token())
              .consume_batch([](const std::vector<Message>&) {
                  std::this_thread::sleep_for(std::chrono::milliseconds(250));
              });

    check(server.count_with_prefix("/api/v1/lease/") == 0,
          "renew_lease() was never called, so nothing may be renewed");
    check(summary.stopped_by == StopReason::Idle, "the loop ends idle, got " + text(summary));
}

// ============================================================================
// A throwing handler is nacked, unless auto_ack(false)
// ============================================================================

// auto_ack(false) leaves settling to the handler, failures included: the
// consumer sends no nack, by design, and the message comes back when its lease
// expires. A throw must not stop consume() either.
void test_a_throwing_handler_is_not_nacked_without_auto_ack() {
    StubServer server(one_claim(popped(1, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    ConsumeSummary summary;
    StopAfter watchdog(5000);
    try {
        summary = client.queue("consume-no-nack").group("workers").auto_ack(false)
                        .wait(false).idle_millis(300).cancel(watchdog.token())
                        .consume([](const Message&) { throw std::runtime_error("boom"); });
    } catch (...) {
        threw = true;
    }

    check(!threw, "a failing handler must not stop consume()");
    auto acks = acks_sent(server);
    check(acks.empty(), "auto_ack(false) sends no nack, got " + json(acks).dump());
    check(summary.processed == 1 && summary.acked == 0 && summary.nacked == 0 &&
          summary.stopped_by == StopReason::Idle,
          "the summary must not claim a nack the client never sent, got " + text(summary));
}

void test_a_throwing_batch_handler_is_not_nacked_without_auto_ack() {
    StubServer server(one_claim(popped(2, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    StopAfter watchdog(5000);
    ConsumeSummary summary =
        client.queue("consume-no-nack-batch").group("workers").auto_ack(false)
              .wait(false).idle_millis(300).cancel(watchdog.token())
              .consume_batch([](const std::vector<Message>&) { throw std::runtime_error("boom"); });

    auto acks = acks_sent(server);
    check(acks.empty(), "auto_ack(false) sends no nack, got " + json(acks).dump());
    check(summary.processed == 2 && summary.acked == 0 && summary.nacked == 0 &&
          summary.stopped_by == StopReason::Idle,
          "the summary must not claim a nack the client never sent, got " + text(summary));
}

// Without auto_ack, the handler acks by itself, and after a failure the loop
// abandons the REST OF THE BATCH -- every partition of it, as the Rust client
// does: the handler's ack of a later message could commit the failed one. The
// whole claim comes back when its lease expires.
// (Before 2.7.1 only the failed message's partition was skipped, and the other
// partition of a multi-partition claim was still handed to the handler.)
void test_without_auto_ack_a_failure_abandons_the_rest_of_the_batch() {
    // t1 and t3 in p1, t2 and t4 in p2, one lease: a multi-partition pop.
    StubServer server(one_claim(popped(4, "lease-1", "workers", {"p1", "p2"})));
    QueenClient client({server.url()}, fast_config());

    std::vector<std::string> handled;
    StopAfter watchdog(5000);
    ConsumeSummary summary =
        client.queue("consume-no-nack-skip").group("workers").auto_ack(false)
              .wait(false).idle_millis(300).cancel(watchdog.token())
              .consume([&](const Message& msg) {
                  handled.push_back(msg.transaction_id);
                  if (msg.transaction_id == "t1") throw std::runtime_error("boom");
                  client.ack(msg);
              });

    check(handled == std::vector<std::string>({"t1"}),
          "nothing of the batch after the failed t1 may be handled, in any partition; handled " +
          json(handled).dump());
    check(settled(server).empty(),
          "no ack of a later message and no nack, got " + json(settled(server)).dump());
    check(summary.processed == 1 && summary.acked == 0 && summary.nacked == 0,
          "one message handled, nothing settled by the loop, got " + text(summary));
}

void test_a_nack_drops_the_later_messages_of_its_partition() {
    StubServer server(one_claim(popped(3, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    std::vector<std::string> handled;
    StopAfter watchdog(5000);
    ConsumeSummary summary =
        client.queue("consume-abandon").group("workers")
              .wait(false).idle_millis(300).cancel(watchdog.token())
              .consume([&handled](const Message& msg) {
                  handled.push_back(msg.transaction_id);
                  if (msg.transaction_id == "t2") throw std::runtime_error("boom");
              });

    check(handled == std::vector<std::string>({"t1", "t2"}),
          "the message after a nack in its partition must not be handled, handled " +
          json(handled).dump());

    auto acks = acks_sent(server);
    check(acks.size() == 2, "expected an ack and a nack, got " + std::to_string(acks.size()) +
          " acknowledgment(s)");
    check(summary.processed == 2 && summary.acked == 1 && summary.nacked == 1 &&
          summary.stopped_by == StopReason::Idle,
          "the summary must count one ack and one nack, got " + text(summary));
    if (acks.size() != 2) return;
    check(acks[0].value("transactionId", "") == "t1" && acks[0].value("status", "") == "completed",
          "t1 must be acked, got " + acks[0].dump());
    check(acks[1].value("transactionId", "") == "t2" && acks[1].value("status", "") == "failed",
          "t2 must be nacked, got " + acks[1].dump());
}

void test_a_nack_leaves_the_other_partitions_of_the_claim_alone() {
    // t1 and t3 in p1, t2 and t4 in p2, one lease: a multi-partition pop.
    StubServer server(one_claim(popped(4, "lease-1", "workers", {"p1", "p2"})));
    QueenClient client({server.url()}, fast_config());

    std::vector<std::string> handled;
    StopAfter watchdog(5000);
    ConsumeSummary summary =
        client.queue("consume-multi-partition").group("workers")
              .wait(false).idle_millis(300).cancel(watchdog.token())
              .consume([&handled](const Message& msg) {
                  handled.push_back(msg.transaction_id);
                  if (msg.transaction_id == "t1") throw std::runtime_error("boom");
              });

    check(handled == std::vector<std::string>({"t1", "t2", "t4"}),
          "p2 keeps its lease and must be handled, p1 after t1 must not; handled " +
          json(handled).dump());

    check(settled(server) == std::vector<std::string>({"t1:failed", "t2:completed", "t4:completed"}),
          "expected t1 nacked and t2, t4 acked, got " + json(settled(server)).dump());
    check(summary.processed == 3 && summary.acked == 2 && summary.nacked == 1,
          "the summary must count two acks and one nack, got " + text(summary));
}

void test_a_nack_survives_an_error_that_is_not_utf8_or_short() {
    StubServer server(one_claim(popped(2, "lease-1", "workers", {"p1", "p2"})));
    QueenClient client({server.url()}, fast_config());

    const std::string latin1 = std::string("cannot open caf") + '\xe9' + ".txt";
    const std::string huge(20000, 'x');
    StopAfter watchdog(5000);
    client.queue("consume-bad-reason").group("workers")
          .wait(false).idle_millis(300).cancel(watchdog.token())
          .consume([&](const Message& msg) {
              throw std::runtime_error(msg.transaction_id == "t1" ? latin1 : huge);
          });

    auto acks = acks_sent(server);
    check(acks.size() == 2, "both messages must be nacked, got " + std::to_string(acks.size()));
    if (acks.size() != 2) return;
    auto error_of = [](const json& ack) {
        return ack.contains("error") && ack["error"].is_string() ? ack["error"].get<std::string>()
                                                                 : std::string();
    };
    std::string first = error_of(acks[0]);
    check(first.rfind("cannot open caf", 0) == 0 && first.find("\xef\xbf\xbd") != std::string::npos,
          "an invalid byte is replaced by U+FFFD, got " + acks[0].dump());
    std::string second = error_of(acks[1]);
    check(second.size() == util::MAX_NACK_ERROR_BYTES,
          "a long error is capped at " + std::to_string(util::MAX_NACK_ERROR_BYTES) +
          " bytes, got " + std::to_string(second.size()));
}

void test_a_handler_that_throws_a_non_standard_value_is_nacked() {
    StubServer server(one_claim(popped(1, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    StopAfter watchdog(5000);
    try {
        client.queue("consume-throw-int").group("workers")
              .wait(false).idle_millis(300).cancel(watchdog.token())
              .consume([](const Message&) { throw 42; });
    } catch (...) {
        threw = true;
    }

    check(!threw, "a failing handler must not stop consume()");
    auto acks = acks_sent(server);
    check(acks.size() == 1 && acks[0].value("status", "") == "failed",
          "the message must be nacked, got " + json(acks).dump());
    check(acks.size() == 1 && acks[0].contains("error") && acks[0]["error"].is_string() &&
          !acks[0]["error"].get<std::string>().empty(),
          "the nack must still carry a reason, got " + json(acks).dump());
}

// ============================================================================
// The loop: limit across workers, concurrency, cancel, and why it stopped
// ============================================================================

// limit() counts across ALL workers: before 2.7.1 every worker had its own count,
// so concurrency(4).limit(8) could handle 32 messages.
void test_limit_counts_across_all_workers() {
    StubServer server(endless(1));
    QueenClient client({server.url()}, fast_config());

    const int workers = 4;
    const long long limit = 8;
    std::mutex mutex;
    std::vector<std::string> handled;
    std::set<std::thread::id> threads;
    StopAfter watchdog(10000);
    ConsumeSummary summary =
        client.queue("consume-limit").group("workers").concurrency(workers).limit(limit)
              .wait(false).cancel(watchdog.token())
              .consume([&](const Message& msg) {
                  {
                      std::lock_guard<std::mutex> lock(mutex);
                      handled.push_back(msg.transaction_id);
                      threads.insert(std::this_thread::get_id());
                  }
                  // Long enough for every worker to be holding a message at once.
                  std::this_thread::sleep_for(std::chrono::milliseconds(30));
              });

    const long long processed = static_cast<long long>(summary.processed);
    check(summary.stopped_by == StopReason::Limit, "the loop must end on its limit, got " +
          text(summary));
    // A worker checks the shared count before each pop and after each message,
    // so the messages the other workers already hold are finished: at most
    // workers - 1 past the limit, never a limit per worker.
    check(processed >= limit && processed <= limit + workers - 1,
          "limit(8) over 4 workers must handle 8 to 11 messages, handled " +
          std::to_string(processed));
    check(static_cast<long long>(handled.size()) == processed &&
          summary.acked == summary.processed && summary.nacked == 0,
          "every handled message is counted and acked once, handled " +
          std::to_string(handled.size()) + ", summary " + text(summary));
    check(threads.size() >= 2, "the limit must be shared by several workers, only " +
          std::to_string(threads.size()) + " thread(s) handled messages");

    // A batch is counted whole: limit(5) over batches of 3 ends after the second.
    StubServer batches(endless(3));
    QueenClient batch_client({batches.url()}, fast_config());
    std::atomic<int> calls{0};
    StopAfter batch_watchdog(5000);
    ConsumeSummary batch_summary =
        batch_client.queue("consume-limit-batch").group("workers").limit(5)
                    .wait(false).cancel(batch_watchdog.token())
                    .consume_batch([&calls](const std::vector<Message>&) { calls++; });
    check(batch_summary.stopped_by == StopReason::Limit && batch_summary.processed == 6 &&
          calls.load() == 2 && batches.count_with_prefix("/api/v1/pop") == 2,
          "limit(5) over batches of 3 must handle two batches and stop, got " +
          text(batch_summary) + " after " + std::to_string(calls.load()) + " call(s)");
}

// concurrency(n) runs n threads at once -- more than the machine has cores.
// Before 2.7.1 the workers ran on a pool sized to the machine, so the workers
// past it waited for others to finish. Every handler here waits until all n
// are inside at the same time.
void test_concurrency_runs_that_many_threads() {
    const unsigned cores = std::max(1u, std::thread::hardware_concurrency());
    const int workers = static_cast<int>(std::max(4u, cores + 2));
    StubServer server(endless(1));
    QueenClient client({server.url()}, fast_config());

    std::mutex mutex;
    std::condition_variable entered;
    int inside = 0;
    int peak = 0;
    std::set<std::thread::id> threads;
    StopAfter watchdog(20000);
    ConsumeSummary summary =
        client.queue("consume-concurrency").group("workers").concurrency(workers).limit(workers)
              .auto_ack(false).wait(false).cancel(watchdog.token())
              .consume([&](const Message&) {
                  std::unique_lock<std::mutex> lock(mutex);
                  threads.insert(std::this_thread::get_id());
                  peak = std::max(peak, ++inside);
                  entered.notify_all();
                  entered.wait_for(lock, std::chrono::seconds(5), [&] { return peak >= workers; });
                  --inside;
              });

    check(peak == workers, "concurrency(" + std::to_string(workers) + ") must run " +
          std::to_string(workers) + " handlers at once, the most at once was " +
          std::to_string(peak));
    check(static_cast<int>(threads.size()) == workers,
          "one thread per worker, got " + std::to_string(threads.size()));
    check(summary.stopped_by == StopReason::Limit, "the loop must end on its limit, got " +
          text(summary));
}

void test_a_cancel_stops_the_loop() {
    {
        // Cancelled before it starts: no request at all. idle_millis is only the
        // backstop of a loop that ignores the token.
        StubServer server([](const httplib::Request&, httplib::Response& res) { res.status = 204; });
        QueenClient client({server.url()}, fast_config());
        Cancel stopped;
        stopped.cancel();
        ConsumeSummary summary = client.queue("consume-cancel-first").group("workers")
                                       .concurrency(4).wait(false).idle_millis(2000).cancel(stopped)
                                       .consume([](const Message&) {});
        check(summary.stopped_by == StopReason::Cancelled && summary.processed == 0,
              "a cancelled token stops the loop before it polls, got " + text(summary));
        check(server.calls().empty(), "a cancelled loop sends nothing, got " +
              std::to_string(server.calls().size()) + " request(s)");
    }
    {
        // Between polls: an empty queue, no limit; idle_millis is only the
        // backstop of a loop that ignores the token.
        StubServer server([](const httplib::Request&, httplib::Response& res) { res.status = 204; });
        QueenClient client({server.url()}, fast_config());
        Cancel token;
        std::thread canceller([token]() mutable {
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            token.cancel();
        });
        const auto started = std::chrono::steady_clock::now();
        ConsumeSummary summary = client.queue("consume-cancel-idle").group("workers")
                                       .concurrency(2).wait(false).idle_millis(5000).cancel(token)
                                       .consume([](const Message&) {});
        const auto took = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started).count();
        canceller.join();
        check(summary.stopped_by == StopReason::Cancelled && summary.processed == 0,
              "cancel() must stop a polling loop, got " + text(summary));
        check(took < 1500, "the loop must stop soon after cancel(), it took " +
              std::to_string(took) + " ms");
    }
    {
        // During a message: the loop finishes it -- handled and acked -- and
        // stops before the next. The rest of the claim stays leased.
        StubServer server(one_claim(popped(3, "lease-1")));
        QueenClient client({server.url()}, fast_config());
        Cancel token;
        std::promise<void> holding;
        std::future<void> held = holding.get_future();
        std::thread canceller([&held, token]() mutable {
            if (held.wait_for(std::chrono::seconds(5)) == std::future_status::ready) token.cancel();
        });
        std::vector<std::string> handled;
        ConsumeSummary summary =
            client.queue("consume-cancel-message").group("workers")
                  .wait(false).idle_millis(3000).cancel(token)
                  .consume([&](const Message& msg) {
                      handled.push_back(msg.transaction_id);
                      if (handled.size() == 1) {
                          holding.set_value();
                          // Still on t1 when the other thread cancels.
                          for (int i = 0; i < 1000 && !token.is_cancelled(); ++i) {
                              std::this_thread::sleep_for(std::chrono::milliseconds(5));
                          }
                      }
                  });
        canceller.join();
        check(handled == std::vector<std::string>({"t1"}),
              "the loop must stop after the message it was on, handled " + json(handled).dump());
        check(settled(server) == std::vector<std::string>({"t1:completed"}),
              "the message it was on is acked, the others are not touched, got " +
              json(settled(server)).dump());
        check(summary.stopped_by == StopReason::Cancelled && summary.processed == 1 &&
              summary.acked == 1,
              "the summary must say Cancelled after one message, got " + text(summary));
    }
}

// ============================================================================
// commit_on_delivery() commits at delivery, on pop() only
// ============================================================================

/// The query of a request target as name -> value. httplib's client sorts the
/// parameters again before it sends them (ClientImpl::write_request parses the
/// query into a std::multimap), so the set of parameters is what a test can
/// pin, not their order.
std::map<std::string, std::string> query_of(const std::string& target) {
    std::map<std::string, std::string> params;
    auto mark = target.find('?');
    if (mark == std::string::npos) return params;
    std::string query = target.substr(mark + 1);
    size_t start = 0;
    while (start < query.size()) {
        size_t end = query.find('&', start);
        std::string pair = query.substr(start, end == std::string::npos ? std::string::npos
                                                                        : end - start);
        auto eq = pair.find('=');
        if (!pair.empty()) {
            params[pair.substr(0, eq)] = eq == std::string::npos ? "" : pair.substr(eq + 1);
        }
        if (end == std::string::npos) break;
        start = end + 1;
    }
    return params;
}

/// The query of the one pop that `configure` sends, to a broker with nothing to
/// give.
std::map<std::string, std::string> pop_query(const std::function<void(QueueBuilder&)>& configure) {
    StubServer server([](const httplib::Request&, httplib::Response& res) { res.status = 204; });
    QueenClient client({server.url()}, fast_config());

    auto builder = client.queue("commit-on-delivery");
    builder.group("workers").wait(false);
    configure(builder);
    builder.pop();

    auto calls = server.calls();
    if (calls.size() != 1) return {{"<requests>", std::to_string(calls.size())}};
    return query_of(calls[0].target);
}

void test_commit_on_delivery_puts_auto_ack_on_the_pop() {
    auto leased = pop_query([](QueueBuilder&) {});
    auto committed = pop_query([](QueueBuilder& b) { b.commit_on_delivery(); });

    check(leased.count("autoAck") == 0,
          "a plain pop must not send autoAck, got " + json(leased).dump());
    check(committed.count("autoAck") == 1 && committed["autoAck"] == "true",
          "commit_on_delivery() must send autoAck=true, got " + json(committed).dump());
    committed.erase("autoAck");
    check(committed == leased,
          "commit_on_delivery() must add autoAck=true and nothing else, got " +
          json(committed).dump() + " against " + json(leased).dump());
}

void test_only_commit_on_delivery_sends_auto_ack() {
    auto off = pop_query([](QueueBuilder& b) { b.commit_on_delivery(false); });
    auto turned_off = pop_query([](QueueBuilder& b) {
        b.commit_on_delivery().commit_on_delivery(false);
    });
    auto acked = pop_query([](QueueBuilder& b) { b.auto_ack(true); });

    check(off.count("autoAck") == 0,
          "commit_on_delivery(false) must not send autoAck, got " + json(off).dump());
    check(turned_off.count("autoAck") == 0,
          "commit_on_delivery(false) must undo commit_on_delivery(), got " +
          json(turned_off).dump());
    check(acked.count("autoAck") == 0,
          "auto_ack() is the consume() handler's ack and must not reach the pop, got " +
          json(acked).dump());
}

void test_consume_refuses_commit_on_delivery_before_any_request() {
    StubServer server(one_claim(popped(1, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    std::atomic<int> handled{0};
    std::string reason;
    std::string batch_reason;
    StopAfter watchdog(3000);
    try {
        client.queue("consume-commit-on-delivery").group("workers").commit_on_delivery()
              .wait(false).idle_millis(300).cancel(watchdog.token())
              .consume([&handled](const Message&) { handled++; });
    } catch (const std::invalid_argument& e) {
        reason = e.what();
    }
    try {
        client.queue("consume-commit-on-delivery").group("workers").commit_on_delivery()
              .wait(false).idle_millis(300).cancel(watchdog.token())
              .consume_batch([&handled](const std::vector<Message>&) { handled++; });
    } catch (const std::invalid_argument& e) {
        batch_reason = e.what();
    }

    check(!reason.empty(), "consume() must refuse commit_on_delivery() with std::invalid_argument");
    check(reason.find("commit_on_delivery") != std::string::npos,
          "the refusal must name the option, got: " + reason);
    check(batch_reason.find("commit_on_delivery") != std::string::npos,
          "consume_batch() must refuse it the same way, got: " + batch_reason);
    check(server.calls().empty(), "the refusal must come before any request, got " +
          std::to_string(server.calls().size()) + " request(s)");
    check(handled.load() == 0, "no message may reach the handler");
}

// ============================================================================

void test_optional_consumer_supervision() {
    for (bool enabled : {false, true}) {
        // Two messages, one per pop, then an empty queue: limit(2) is shared by
        // the two workers, so the loop handles exactly these two, whichever
        // worker takes them.
        auto pops = std::make_shared<std::atomic<int>>(0);
        StubServer server([pops](const httplib::Request& req, httplib::Response& res) {
            if (req.path == "/api/v1/kv") {
                res.set_content(R"({"results":[{"applied":true},{"applied":true}]})", "application/json");
            } else if ((*pops)++ < 2) {
                res.set_content(popped(1, "lease-1"), "application/json");
            } else {
                res.status = 204;
            }
        });
        QueenClient client({server.url()}, fast_config());
        StopAfter watchdog(5000);
        auto builder = client.queue("orders").concurrency(2).limit(2).auto_ack(false).wait(false)
                             .cancel(watchdog.token());
        if (enabled) builder.supervision(SupervisionConfig{"billing-production"});
        std::atomic<int> calls{0};
        ConsumeSummary summary =
            builder.consume([&](const Message&) { if (++calls == 1) throw std::runtime_error("private failure"); });
        std::vector<json> docs;
        for (const auto& call : server.calls()) if (call.path == "/api/v1/kv") {
            auto ops = json::parse(call.body).at("operations");
            check(ops.size() == 2, "status must be an atomic head/chunk batch");
            check(ops[0]["ns"] == "queen-supervisor" && ops[0]["ttlSeconds"] == 60 && ops[1]["ttlSeconds"] == 60, "namespace and TTL");
            check(ops[0]["value"]["write"] == ops[1]["value"]["write"], "generation must match");
            auto raw = util::base64_decode(ops[1]["value"]["data"].get<std::string>());
            check(raw.size() == ops[0]["value"]["bytes"].get<size_t>(), "UTF-8 byte length");
            check(raw.find("private failure") == std::string::npos, "error text must not be published");
            docs.push_back(json::parse(raw));
        }
        check(calls == 2, "publication must not affect consumption");
        check(summary.stopped_by == StopReason::Limit && summary.processed == 2,
              "the shared limit ends the loop, got " + text(summary));
        check(server.count_with_prefix("/api/v1/ack") == 0, "manual ACK policy must be preserved");
        if (!enabled) { check(docs.empty(), "default off must not publish"); continue; }
        check(docs.size() >= 2, "lifecycle observations");
        auto last = docs.back(); const auto& pool = last["pool_status"][0];
        check(last["state"] == "stopped", "final state");
        check(pool["running"] == 0 && pool["busy"] == 0, "actual exits");
        check(pool["completed"] == 1 && pool["failed"] == 1, "handler outcomes");
    }
}

void test_supervision_progress_and_validation() {
    ConsumeOptions options; options.queue = "orders"; options.concurrency = 2;
    options.supervision = SupervisionConfig{"billing"};
    ConsumerSupervision reporter(nullptr, options), other(nullptr, options);
    check(reporter.document("running")["instance_id"] != other.document("running")["instance_id"], "unique instance identity");
    reporter.worker_started(); auto id = reporter.begin_handler();
    auto pool = reporter.document("running")["pool_status"][0];
    check(pool["running"] == 1 && pool["busy"] == 1 && pool["completed"] == 0, "only started threads count as running");
    reporter.end_handler(id, true); reporter.worker_exited();
    pool = reporter.document("running")["pool_status"][0];
    check(pool["running"] == 0 && pool["completed"] == 1 && pool["oldest_inflight_seconds"].is_null(), "progress and exit cleanup");
    for (const auto& group : {"", "coordination", "a/b", "a\n"}) {
        options.supervision = SupervisionConfig{group}; bool refused = false;
        try { ConsumerSupervision invalid(nullptr, options); } catch (const std::invalid_argument&) { refused = true; }
        check(refused, "invalid publication group must fail");
    }
}

void test_supervision_publication_is_bounded() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        if (req.path == "/api/v1/kv") {
            check(req.get_header_value("Authorization") == "Bearer test-token", "publication uses existing credentials");
            std::this_thread::sleep_for(std::chrono::seconds(3));
            res.status = 429; res.set_header("Retry-After", "1000");
        } else { res.set_content(popped(1, "lease-1"), "application/json"); }
    });
    auto config = fast_config(); config.bearer_token = "test-token";
    QueenClient client({server.url()}, config);
    const auto started = std::chrono::steady_clock::now();
    int handled = 0;
    StopAfter watchdog(10000);
    client.queue("orders").supervision(SupervisionConfig{"billing"}).auto_ack(false).wait(false).limit(1)
        .cancel(watchdog.token())
        .consume_batch([&](const std::vector<Message>&) { ++handled; });
    check(handled == 1, "failed publication cannot stop the handler");
    check(std::chrono::steady_clock::now() - started < std::chrono::seconds(6), "publication cannot inherit unbounded retries");
    check(server.count_with_prefix("/api/v1/kv") == 2, "exactly one attempt per lifecycle publication");
}

int main() {
    run_test("optional supervision reports actual consumer lifecycle", test_optional_consumer_supervision);
    run_test("supervision observes progress and validates groups", test_supervision_progress_and_validation);
    run_test("supervision publication is bounded", test_supervision_publication_is_bounded);

    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - Consume Contract Tests" << std::endl;
    std::cout << "========================================\n" << std::endl;

    run_test("ack() reports a refused acknowledgment", test_ack_reports_a_refused_acknowledgment);
    run_test("ack() reports an accepted acknowledgment",
             test_ack_reports_an_accepted_acknowledgment);
    run_test("ack() takes the group and the lease from the message",
             test_ack_takes_the_group_and_the_lease_from_the_message);
    run_test("ack() and renew() throw when the call could not be made",
             test_ack_and_renew_throw_when_the_call_could_not_be_made);
    run_test("renew() reports a lease the broker did not extend",
             test_renew_reports_a_lease_the_broker_did_not_extend);
    run_test("renew() of a batch renews each lease once",
             test_renew_of_a_batch_renews_each_lease_once);

    run_test("pop() throws instead of returning nothing",
             test_pop_throws_instead_of_returning_nothing);

    run_test("a 4xx on a pop stops consume() with that error",
             test_a_4xx_on_a_pop_stops_consume_with_that_error);
    run_test("a 403 on a pop stops every worker with that error",
             test_a_403_on_a_pop_stops_every_worker_with_that_error);
    run_test("a 5xx keeps consume() polling", test_a_5xx_keeps_consume_polling);
    run_test("a network fault keeps consume() polling", test_a_network_fault_keeps_consume_polling);

    run_test("renew_lease() renews while the handler runs",
             test_renew_lease_renews_while_the_handler_runs);
    run_test("no renewal unless asked", test_no_renewal_unless_asked);

    run_test("a throwing handler is not nacked without auto_ack",
             test_a_throwing_handler_is_not_nacked_without_auto_ack);
    run_test("a throwing batch handler is not nacked without auto_ack",
             test_a_throwing_batch_handler_is_not_nacked_without_auto_ack);
    run_test("without auto_ack a failure abandons the rest of the batch",
             test_without_auto_ack_a_failure_abandons_the_rest_of_the_batch);
    run_test("a nack drops the later messages of its partition",
             test_a_nack_drops_the_later_messages_of_its_partition);
    run_test("a nack leaves the other partitions of the claim alone",
             test_a_nack_leaves_the_other_partitions_of_the_claim_alone);
    run_test("a nack survives an error that is not UTF-8 or short",
             test_a_nack_survives_an_error_that_is_not_utf8_or_short);
    run_test("a handler that throws a non-standard value is nacked",
             test_a_handler_that_throws_a_non_standard_value_is_nacked);

    run_test("limit() counts across all workers", test_limit_counts_across_all_workers);
    run_test("concurrency(n) runs n threads at once", test_concurrency_runs_that_many_threads);
    run_test("a Cancel stops the loop", test_a_cancel_stops_the_loop);

    run_test("commit_on_delivery() puts autoAck=true on the pop",
             test_commit_on_delivery_puts_auto_ack_on_the_pop);
    run_test("only commit_on_delivery() sends autoAck", test_only_commit_on_delivery_sends_auto_ack);
    run_test("consume() refuses commit_on_delivery() before any request",
             test_consume_refuses_commit_on_delivery_before_any_request);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All consume contract tests passed" << RESET << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
