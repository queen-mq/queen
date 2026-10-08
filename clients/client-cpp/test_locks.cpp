/**
 * Queen C++ Client - locks, semaphores, `check` and guarded transactions
 *
 * Two halves, in one binary:
 *
 *   make locks && ./bin/test_locks                          # wire only
 *   ./bin/test_locks http://localhost:6632                  # wire + a live broker
 *
 * THE WIRE HALF asserts the EXACT JSON of every request against an in-process
 * httplib::Server, for the reason test_kv_timers.cpp gives: the body is the
 * contract, and a misspelled field is a 400 nobody can diagnose from here.
 * What lives in the CLIENT and nowhere else, and is pinned there:
 *   - the handle always sends its owner, the same one on every call;
 *   - a renew's NEW token replaces the old one for the guard and the release;
 *   - keep_alive() sends nothing until a third of the lifetime has passed;
 *   - a guard that loses is the verdict, and the handle then holds nothing;
 *   - a transaction that asked for a guard never goes out without one.
 *
 * THE LIVE HALF runs only with a server URL, and shows what only a broker can:
 * one holder; a call sent again by its owner is the same permit; an expired
 * lock goes to the next handle with a higher token and the old handle's guarded
 * step pushes nothing; a semaphore never grants more than its limit.
 */

#include "queen_client.hpp"
#include <algorithm>
#include <functional>
#include <iostream>
#include <stdexcept>
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
// Test double: records every request and answers from a script
// ============================================================================

struct RecordedCall {
    std::string method;
    std::string target;
    std::string body;
};

/// Serves `plan` in request order, then `fallback` forever.
class PlanServer {
private:
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    mutable std::mutex mutex_;
    std::vector<RecordedCall> calls_;
    std::vector<json> plan_;
    json fallback_;
    size_t index_ = 0;

public:
    explicit PlanServer(std::vector<json> plan, json fallback = json::object())
        : plan_(std::move(plan)), fallback_(std::move(fallback)) {
        auto handler = [this](const httplib::Request& req, httplib::Response& res) {
            json answer;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                calls_.push_back({req.method, req.target, req.body});
                answer = index_ < plan_.size() ? plan_[index_] : fallback_;
                ++index_;
            }
            res.status = 200;
            res.set_content(answer.dump(), "application/json");
        };
        server_.Get(".*", handler);
        server_.Post(".*", handler);
        port_ = server_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this]() { server_.listen_after_bind(); });
        server_.wait_until_ready();
    }

    ~PlanServer() {
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

    /// The first operation of request `i`, or the whole body for a transaction.
    json op(size_t i) const {
        auto all = calls();
        json body = json::parse(all.at(i).body);
        return body.contains("operations") && !body.contains("requiredLeases")
                   ? body["operations"][0]
                   : body;
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

/// Exact JSON equality: an extra field is as much a contract break as a
/// missing one. Key order is not part of the contract.
void check_json(const json& actual, const std::string& expected, const std::string& what) {
    json want = json::parse(expected);
    check(actual == want, what + "\n      expected: " + want.dump() + "\n      actual:   " + actual.dump());
}

template <typename Fn>
void check_throws(Fn fn, const std::string& what) {
    try {
        fn();
        check(false, what + " (nothing was thrown)");
    } catch (const std::exception&) {
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
    std::cout << (failures == before ? GREEN "PASS: " : RED "FAIL: ") << name << RESET << std::endl;
}

json guard_of(const std::string& name, int slot, long long token) {
    return {{"op", "check"}, {"ns", "queen-locks"}, {"key", name + "#" + std::to_string(slot)},
            {"expect", token}, {"required", true}};
}

json results(const json& element) { return {{"results", json::array({element})}}; }

json granted(const std::string& name, long long token, int slot = 0) {
    return results({{"index", 0}, {"op", "acquire"}, {"name", name}, {"acquired", true},
                    {"slot", slot}, {"token", token}, {"owner", "o"},
                    {"guard", guard_of(name, slot, token)}});
}

json refused(const std::string& name) {
    return results({{"index", 0}, {"op", "acquire"}, {"name", name}, {"acquired", false},
                    {"reason", "held"},
                    {"holders", json::array({{{"slot", 0}, {"owner", "other"}}})}});
}

json renewed(const std::string& name, long long token, int slot = 0) {
    return results({{"index", 0}, {"op", "renew"}, {"name", name}, {"renewed", true},
                    {"slot", slot}, {"token", token}, {"guard", guard_of(name, slot, token)}});
}

json released(const std::string& name) {
    return results({{"index", 0}, {"op", "release"}, {"name", name}, {"released", true}, {"slot", 0}});
}

json committed() { return {{"transactionId", "t"}, {"success", true}, {"results", json::array()}}; }

json lost_to(int failed_index, const std::string& kv_reason, const json& value, long long version) {
    return {{"transactionId", "t"}, {"success", false}, {"reason", "kv_precondition"},
            {"error", "QKV"}, {"results", json::array()}, {"ok", false},
            {"failedIndex", failed_index}, {"kvReason", kv_reason}, {"value", value},
            {"version", version}};
}

// ============================================================================
// Wire: check
// ============================================================================

void test_check_op_shapes() {
    check_json(wire::kv_check_op("orders", "k", 7, false),
               R"({"op":"check","ns":"orders","key":"k","expect":7})", "a check is its expect");
    check_json(wire::kv_check_op("queen-locks", "job#0", 41, true),
               R"({"op":"check","ns":"queen-locks","key":"job#0","expect":41,"required":true})",
               "the guard a lock hands out");
    // expect:0 is "must not exist" and reaches the wire as 0.
    check(wire::kv_check_op("orders", "k", 0, false)["expect"] == 0, "expect 0 is sent");
    check_throws([] { wire::kv_check_op("orders", "k", -1, false); }, "a negative version");
}

void test_check_posts_to_the_kv_route_and_rides_a_transaction() {
    PlanServer server({results({{"index", 0}, {"op", "check"}, {"applied", true}, {"key", "k"},
                                {"version", 7}}),
                       committed()});
    QueenClient client(server.url());

    json held = client.kv("orders").check("k", 7);
    check(server.calls()[0].target == "/api/v1/kv", "check goes to the kv route");
    check_json(json::parse(server.calls()[0].body),
               R"({"operations":[{"op":"check","ns":"orders","key":"k","expect":7}]})",
               "the standalone check body");
    check(held["applied"] == true && !held.contains("value"), "a held check hands back no value");

    client.transaction()
        .kv("queen-locks").check("job#0", 41)
        .kv("work").put("state", 1, KvTtl::forever())
        .commit();
    json body = json::parse(server.calls()[1].body);
    check(body["kv"][0] == guard_of("job", 0, 41),
          "a check in a bundle is required by default: " + body["kv"][0].dump());
}

// ============================================================================
// Wire: the four operations
// ============================================================================

void test_lock_op_shapes() {
    check_json(wire::lock_acquire_op("daily-report", 30, "o", 1),
               R"({"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"o"})",
               "a lock sends no limit");
    check_json(wire::lock_acquire_op("gpu", 60, "", 4),
               R"({"op":"acquire","name":"gpu","ttlSeconds":60,"limit":4})", "a semaphore's acquire");
    check_json(wire::lock_renew_op("gpu", 100, 60, 2, "o"),
               R"({"op":"renew","name":"gpu","token":100,"ttlSeconds":60,"slot":2,"owner":"o"})",
               "a renew");
    check_json(wire::lock_renew_op("job", 100, 30, 0, ""),
               R"({"op":"renew","name":"job","token":100,"ttlSeconds":30})",
               "slot 0 is the default and is not sent");
    check_json(wire::lock_release_op("daily-report", 101, 0),
               R"({"op":"release","name":"daily-report","token":101})", "a release");
    check_json(wire::lock_release_op("gpu", 9, 3),
               R"({"op":"release","name":"gpu","token":9,"slot":3})", "a semaphore's release");
    check_json(wire::lock_get_op("daily-report"), R"({"op":"get","name":"daily-report"})", "a get");
}

void test_what_the_broker_would_refuse_is_refused_before_the_request() {
    check_throws([] { wire::lock_acquire_op("a", 0, "", 1); }, "no lifetime");
    check_throws([] { wire::lock_acquire_op("a#b", 1, "", 1); }, "a # in the name");
    check_throws([] { wire::lock_acquire_op("", 1, "", 1); }, "an empty name");
    check_throws([] { wire::lock_acquire_op("a\nb", 1, "", 1); }, "a control character");
    check_throws([] { wire::lock_acquire_op("a", 1, "", 0); }, "a limit of zero");
    check_throws([] { wire::lock_acquire_op("a", 1, "", 1025); }, "a limit past the ceiling");
    check_throws([] { wire::lock_renew_op("a", 0, 1, 0, ""); }, "a renew with no token");
    check_throws([] { wire::lock_release_op("a", 0, 0); }, "a release with no token");
    PlanServer server({});
    QueenClient client(server.url());
    check_throws([&] { client.lock("a", 0); }, "a handle with no lifetime");
    check(server.count() == 0, "nothing was sent");
}

void test_the_wire_answers_verdicts_as_fields() {
    PlanServer server({granted("daily-report", 100), refused("daily-report"),
                       renewed("daily-report", 101), released("daily-report"),
                       results({{"index", 0}, {"op", "get"}, {"name", "daily-report"},
                                {"held", false}, {"holders", json::array()}})});
    QueenClient client(server.url());
    auto locks = client.locks();

    json a = locks.acquire("daily-report", 30, "o");
    check(a["acquired"] == true && a["token"] == 100, "acquire: " + a.dump());
    check(a["guard"] == guard_of("daily-report", 0, 100), "the guard comes back");
    json b = locks.acquire("daily-report", 30, "p");
    check(b["acquired"] == false && b["reason"] == "held", "held is a verdict, not an exception");
    json r = locks.renew("daily-report", 100, 30);
    check(r["token"] == 101, "a renew answers a NEW token");
    check(locks.release("daily-report", 101)["released"] == true, "release");
    check(locks.get("daily-report")["held"] == false, "get");

    auto calls = server.calls();
    check(calls[0].method == "POST" && calls[0].target == "/api/v1/locks", "the route");
    check_json(json::parse(calls[0].body),
               R"({"operations":[{"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"o"}]})",
               "the acquire body");
}

void test_a_short_answer_is_loud() {
    PlanServer server({json{{"results", json::array()}}});
    QueenClient client(server.url());
    check_throws([&] { client.locks().get("a"); }, "a missing result");
}

// ============================================================================
// Wire: the handle
// ============================================================================

void test_the_handle_names_its_owner_and_follows_a_renew() {
    PlanServer server({refused("job"), granted("job", 100), renewed("job", 101), released("job")});
    QueenClient client(server.url());
    Lock lock = client.lock("job", 30);
    Lock other = client.lock("job", 30);
    check(lock.owner() != other.owner(), "an owner per handle");
    LockOptions named;
    named.owner = "cron-7";
    check(client.lock("job", 30, named).owner() == "cron-7", "a caller's own owner");

    check(!lock.held() && lock.token() == 0 && lock.slot() == -1, "a new handle holds nothing");
    check(!lock.acquire(), "held by somebody else is false");
    check_json(server.op(0),
               R"({"op":"acquire","name":"job","ttlSeconds":30,"owner":")" + lock.owner() + R"("})",
               "the handle sends its owner");

    check(lock.acquire(), "the second attempt");
    check(server.op(1)["owner"] == lock.owner(), "the same owner on the retry");
    check(lock.held() && lock.token() == 100 && lock.slot() == 0, "held, with its token");
    check(lock.guard() == guard_of("job", 0, 100), "the guard");
    check(lock.acquire() && server.count() == 2, "already held: no call");

    check(lock.renew(), "renew");
    check(lock.token() == 101 && lock.guard() == guard_of("job", 0, 101),
          "the NEW token is in the guard");
    check(server.op(2)["token"] == 100, "the renew carried the token before it");

    check(lock.release(), "release");
    check_json(server.op(3), R"({"op":"release","name":"job","token":101})",
               "the release carries the current token");
    check(!lock.held() && !lock.release() && server.count() == 4, "nothing left to give back");
    check_throws([&] { lock.guard(); }, "a guard of an unheld lock");
}

void test_a_semaphore_handle_sends_its_limit_and_keeps_its_slot() {
    PlanServer server({granted("gpu", 9, 3), released("gpu")});
    QueenClient client(server.url());
    Lock permit = client.semaphore("gpu", 4, 60);
    check(permit.limit() == 4 && permit.acquire(), "acquire");
    check(server.op(0)["limit"] == 4, "the limit is sent");
    check(permit.slot() == 3, "its slot");
    permit.release();
    check_json(server.op(1), R"({"op":"release","name":"gpu","token":9,"slot":3})",
               "the release names the slot");
}

void test_acquire_with_a_wait_comes_back() {
    LockOptions fast;
    fast.retry_min_millis = 5;
    fast.retry_max_millis = 10;
    {
        PlanServer server({refused("job"), refused("job"), granted("job", 5)});
        QueenClient client(server.url());
        Lock lock = client.lock("job", 30, fast);
        check(lock.acquire(5000) && server.count() == 3, "it came back until the permit was free");
    }
    {
        PlanServer server({}, refused("job"));
        QueenClient client(server.url());
        Lock lock = client.lock("job", 30, fast);
        check(!lock.acquire(60) && server.count() >= 2, "the wait ran out");
    }
}

void test_a_refused_renew_is_a_loss() {
    PlanServer server({granted("job", 100),
                       results({{"index", 0}, {"op", "renew"}, {"name", "job"}, {"renewed", false},
                                {"reason", "lost"}, {"slot", 0}, {"holders", json::array()}})});
    QueenClient client(server.url());
    Lock lock = client.lock("job", 30);
    lock.acquire();
    check(!lock.renew() && !lock.held() && lock.token() == 0, "a refused renew");
    check(!lock.release() && server.count() == 2, "nothing to give back, and no call");
}

void test_keep_alive_renews_only_when_due_and_a_lifetime_runs_out() {
    {
        PlanServer server({granted("job", 100), renewed("job", 101)});
        QueenClient client(server.url());
        Lock lock = client.lock("job", 3);
        lock.acquire();
        check(lock.keep_alive() && lock.keep_alive() && server.count() == 1,
              "not due yet: nothing was sent");
        std::this_thread::sleep_for(std::chrono::milliseconds(1050));
        check(lock.keep_alive() && server.count() == 2 && lock.token() == 101,
              "a third of the lifetime passed: one renew");
    }
    {
        PlanServer server({granted("job", 100)});
        QueenClient client(server.url());
        Lock lock = client.lock("job", 1);
        lock.acquire();
        check(lock.held(), "held");
        std::this_thread::sleep_for(std::chrono::milliseconds(1050));
        check(!lock.held() && lock.token() == 0,
              "past its deadline a handle does not claim to hold");
        check(!lock.keep_alive() && server.count() == 1, "and keep_alive says stop, without a call");
    }
}

// ============================================================================
// Wire: guard(lock) on a transaction
// ============================================================================

void test_the_guard_is_the_first_kv_op_at_the_token_held_when_commit_sends() {
    PlanServer server({granted("job", 100), renewed("job", 101), committed()});
    QueenClient client(server.url());
    Lock lock = client.lock("job", 30);
    lock.acquire();
    auto tx = client.transaction();
    tx.guard(lock).kv("work").put("state", json{{"n", 1}}, KvTtl::forever());
    lock.renew(); // after the guard was asked for, before commit
    json result = tx.commit();
    check(result["success"] == true, "committed");
    check(server.calls()[2].target == "/api/v1/transaction", "the transaction route");
    json body = json::parse(server.calls()[2].body);
    check(body["kv"].size() == 2 && body["kv"][0] == guard_of("job", 0, 101),
          "the guard is first, at the renewed token: " + body["kv"].dump());
}

void test_a_guard_that_loses_is_the_verdict_and_the_handle_holds_nothing() {
    // Two pushed items come first in the flat index space: the guard is 2.
    PlanServer server({granted("job", 100),
                       lost_to(2, "version", {{"owner", "somebody-else"}}, 250)});
    QueenClient client(server.url());
    Lock lock = client.lock("job", 30);
    lock.acquire();
    json result = client.transaction()
        .guard(lock)
        .queue("reports").push({json{{"data", 1}}, json{{"data", 2}}})
        .commit();
    check(result["success"] == false && result["reason"] == "kv_precondition",
          "returned, not thrown");
    check(!lock.held(), "the handle holds nothing");
}

void test_a_precondition_that_is_not_the_guards_leaves_the_lock_alone() {
    // kv = [guard, marker]: flat index 1 is the bundle's own gate.
    PlanServer server({granted("job", 100), lost_to(1, "exists", true, 77)});
    QueenClient client(server.url());
    Lock lock = client.lock("job", 30);
    lock.acquire();
    KvWriteOptions gate;
    gate.required = true;
    json result = client.transaction()
        .guard(lock)
        .kv("idem").put_if_absent("order-1", true, KvTtl::seconds(3600), gate)
        .commit();
    check(result["success"] == false && lock.held(), "the marker lost, not the lock");
}

void test_a_step_that_asked_for_a_guard_never_goes_out_without_one() {
    PlanServer server({});
    QueenClient client(server.url());
    Lock lock = client.lock("job", 30);
    bool refused_before_sending = false;
    try {
        client.transaction().guard(lock).kv("w").put("k", 1, KvTtl::forever()).commit();
    } catch (const LockNotHeldError&) {
        refused_before_sending = true;
    }
    check(refused_before_sending && server.count() == 0, "an unheld lock cannot guard");
}

// ============================================================================
// Live: against a real broker
// ============================================================================

static std::string live_url;

std::string unique(const std::string& base) {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return "test-cpp-" + base + "-" + std::to_string(ns);
}

/// A broker alone raises its cluster version to 5 on its first tick; `check`
/// needs it.
void checks_are_served(QueenClient& client) {
    for (int i = 0; i < 100; ++i) {
        try {
            client.kv("test-cpp-probe").check("k", 0);
            return;
        } catch (const HttpError& e) {
            if (e.status_code() != 503) throw;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    throw std::runtime_error("the broker never served a check (cluster version below 5?)");
}

std::vector<json> drain(QueenClient& client, const std::string& queue) {
    std::vector<json> seen;
    for (;;) {
        auto messages = client.queue(queue).batch(50).wait(false).pop();
        if (messages.empty()) {
            return seen;
        }
        for (const auto& m : messages) {
            seen.push_back(m["data"]);
        }
        client.ack(messages, true);
    }
}

void live_a_lock_has_one_holder_and_its_guarded_step_commits() {
    QueenClient client(live_url);
    checks_are_served(client);
    std::string name = unique("one-holder");
    std::string queue = unique("one-holder-q");
    Lock a = client.lock(name, 30);
    Lock b = client.lock(name, 30);

    check(a.acquire(), "the first handle acquires");
    check(!b.acquire(), "a second handle does not acquire a held lock");
    long long token = a.token();

    json who = client.locks().get(name);
    check(who["held"] == true && who["holders"][0]["owner"] == a.owner() &&
          who["holders"][0]["token"] == token, "get shows the holder: " + who.dump());
    // The permit is a KV row and nothing else.
    json row = client.kv(LOCK_NAMESPACE).get(name + "#0");
    check(row["found"] == true && row["version"] == token && row["value"]["owner"] == a.owner(),
          "the permit's row: " + row.dump());

    json result = client.transaction()
        .guard(a)
        .queue(queue).push({json{{"data", {{"step", 1}}}}})
        .commit();
    check(result["success"] == true, "the holder's guarded step commits");
    auto got = drain(client, queue);
    check(got.size() == 1 && got[0]["step"] == 1, "the one guarded message exists");

    check(a.release(), "release");
    check(b.acquire() && b.token() > token, "free after its release, with a higher token");
    b.release();
}

void live_a_call_sent_again_by_its_owner_is_the_same_permit() {
    QueenClient client(live_url);
    std::string name = unique("retry-owner");
    auto locks = client.locks();
    json first = locks.acquire(name, 30, "me");
    json again = locks.acquire(name, 30, "me");
    check(first["acquired"] == true && again["acquired"] == true &&
          again.value("already", false) && again["token"] == first["token"],
          "the retry answers the SAME permit: " + again.dump());

    long long token = first["token"].get<long long>();
    json renewed_once = locks.renew(name, token, 30, 0, "me");
    // The renew's answer is lost; the old token is sent again.
    json resent = locks.renew(name, token, 30, 0, "me");
    check(renewed_once["renewed"] == true && resent["renewed"] == true &&
          resent["token"].get<long long>() > renewed_once["token"].get<long long>(),
          "a renew sent again by its owner is carried through");
    json stranger = locks.renew(name, token, 30, 0, "somebody-else");
    check(stranger["renewed"] == false && stranger["reason"] == "lost" &&
          stranger["holders"][0]["owner"] == "me", "a stranger's stale token is lost");
    check(locks.release(name, resent["token"].get<long long>())["released"] == true, "release");
}

void live_an_expired_lock_is_taken_over_and_the_old_holder_commits_nothing() {
    QueenClient client(live_url);
    checks_are_served(client);
    std::string name = unique("expiry");
    std::string queue = unique("expiry-q");
    // The old holder never calls keep_alive(): it is "paused" past its lease.
    Lock old = client.lock(name, 1);
    Lock next = client.lock(name, 30);

    check(old.acquire(), "the old holder acquires");
    long long old_token = old.token();
    json stale_guard = old.guard();
    std::this_thread::sleep_for(std::chrono::milliseconds(1300));
    check(!old.held(), "past its lifetime a handle does not claim to hold");

    check(next.acquire() && next.token() > old_token,
          "an expired lock goes to the next holder, with a higher token");

    // The old holder wakes up and sends the step it was about to send.
    json stale = client.transaction()
        .kv(stale_guard["ns"].get<std::string>())
            .check(stale_guard["key"].get<std::string>(), stale_guard["expect"].get<long long>())
        .queue(queue).push({json{{"data", {{"from", "old"}}}}})
        .commit();
    check(stale["success"] == false && stale["reason"] == "kv_precondition",
          "the old holder's step rolls back: " + stale.dump());

    json ok = client.transaction()
        .guard(next)
        .queue(queue).push({json{{"data", {{"from", "next"}}}}})
        .commit();
    check(ok["success"] == true, "the new holder's step commits");
    auto got = drain(client, queue);
    check(got.size() == 1 && got[0]["from"] == "next", "only the new holder's message exists");
    next.release();
}

void live_keep_alive_carries_a_lock_past_its_first_lifetime() {
    QueenClient client(live_url);
    Lock lock = client.lock(unique("keepalive"), 2);
    check(lock.acquire(), "acquire");
    long long first = lock.token();
    auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
    bool kept = true;
    while (std::chrono::steady_clock::now() < end) {
        kept = kept && lock.keep_alive();
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    check(kept && lock.held() && lock.token() > first,
          "held for longer than one lifetime, renewed along the way");
    check(lock.release(), "release");
}

void live_a_semaphore_never_grants_more_than_its_limit() {
    QueenClient client(live_url);
    std::string name = unique("sem");
    const int limit = 3;
    std::vector<Lock> permits;
    for (int i = 0; i < 6; ++i) {
        permits.push_back(client.semaphore(name, limit, 30));
    }
    std::vector<size_t> holders;
    for (size_t i = 0; i < permits.size(); ++i) {
        if (permits[i].acquire()) {
            holders.push_back(i);
        }
    }
    check(holders.size() == static_cast<size_t>(limit), "one at a time, exactly the limit is granted");
    std::vector<int> slots;
    for (size_t i : holders) {
        slots.push_back(permits[i].slot());
    }
    std::sort(slots.begin(), slots.end());
    check(slots == std::vector<int>({0, 1, 2}), "one holder per slot");
    check(client.locks().get(name)["holders"].size() == static_cast<size_t>(limit),
          "get lists the holders");

    int freed = permits[holders[0]].slot();
    check(permits[holders[0]].release(), "one leaves");
    Lock extra = client.semaphore(name, limit, 30);
    check(extra.acquire() && extra.slot() == freed, "the next caller gets exactly that slot");
    extra.release();
    permits[holders[1]].release();
    permits[holders[2]].release();
}

// ============================================================================

int main(int argc, char** argv) {
    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - Lock Tests" << std::endl;
    std::cout << "========================================\n" << std::endl;

    run_test("check op shapes", test_check_op_shapes);
    run_test("check posts to the kv route and rides a transaction",
             test_check_posts_to_the_kv_route_and_rides_a_transaction);
    run_test("lock op shapes", test_lock_op_shapes);
    run_test("what the broker would refuse is refused before the request",
             test_what_the_broker_would_refuse_is_refused_before_the_request);
    run_test("the wire answers verdicts as fields", test_the_wire_answers_verdicts_as_fields);
    run_test("a short answer is loud", test_a_short_answer_is_loud);
    run_test("the handle names its owner and follows a renew",
             test_the_handle_names_its_owner_and_follows_a_renew);
    run_test("a semaphore handle sends its limit and keeps its slot",
             test_a_semaphore_handle_sends_its_limit_and_keeps_its_slot);
    run_test("acquire with a wait comes back", test_acquire_with_a_wait_comes_back);
    run_test("a refused renew is a loss", test_a_refused_renew_is_a_loss);
    run_test("keep_alive renews only when due, and a lifetime runs out",
             test_keep_alive_renews_only_when_due_and_a_lifetime_runs_out);
    run_test("the guard is the first kv op, at the token held when commit sends",
             test_the_guard_is_the_first_kv_op_at_the_token_held_when_commit_sends);
    run_test("a guard that loses is the verdict, and the handle holds nothing",
             test_a_guard_that_loses_is_the_verdict_and_the_handle_holds_nothing);
    run_test("a precondition that is not the guard's leaves the lock alone",
             test_a_precondition_that_is_not_the_guards_leaves_the_lock_alone);
    run_test("a step that asked for a guard never goes out without one",
             test_a_step_that_asked_for_a_guard_never_goes_out_without_one);

    if (argc > 1) {
        live_url = argv[1];
        std::cout << "\n--- live broker: " << live_url << " ---\n" << std::endl;
        run_test("live: a lock has one holder and its guarded step commits",
                 live_a_lock_has_one_holder_and_its_guarded_step_commits);
        run_test("live: a call sent again by its owner is the same permit",
                 live_a_call_sent_again_by_its_owner_is_the_same_permit);
        run_test("live: an expired lock is taken over and the old holder commits nothing",
                 live_an_expired_lock_is_taken_over_and_the_old_holder_commits_nothing);
        run_test("live: keep_alive carries a lock past its first lifetime",
                 live_keep_alive_carries_a_lock_past_its_first_lifetime);
        run_test("live: a semaphore never grants more than its limit",
                 live_a_semaphore_never_grants_more_than_its_limit);
    } else {
        std::cout << "\n(no server URL given: the live half was not run)" << std::endl;
    }

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All lock tests passed" << RESET << std::endl;
        return 0;
    }
    std::cout << RED << failures << " lock check(s) failed" << RESET << std::endl;
    return 1;
}
