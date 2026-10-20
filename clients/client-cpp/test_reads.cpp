/**
 * Queen C++ Client - reads by offset (fetch) and partition discovery
 * (changed_since)
 *
 * Two halves, in one binary:
 *
 *   make reads && ./bin/test_reads                          # wire only
 *   ./bin/test_reads http://localhost:6632                  # wire + a live broker
 *
 * THE WIRE HALF runs against an in-process httplib::Server that answers
 * POST /api/v1/fetch and POST /api/v1/partitions/changed the way
 * server/src/rsm/facade/real/phase2/reads.rs does: entries answered in order,
 * each entry's byte budget and the whole call's ceiling (the first record of an
 * entry read whatever it weighs), a record cap per entry, OFFSET_OUT_OF_RANGE
 * below logStartOffset and above the high watermark, UNKNOWN_TOPIC_OR_PARTITION
 * for a queue that does not exist, discovery pages with a `next` cursor exactly
 * when a page is full, BAD_CURSOR for a cursor it did not issue. Every request
 * body is recorded.
 *
 * THE LIVE HALF runs only with a server URL.
 */

#include "queen_client.hpp"
#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <regex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace queen;
using json = nlohmann::json;

#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define RESET "\033[0m"

static const char* TS = "2026-10-20T10:00:00.000000Z";
static const int64_t MIB = 1024 * 1024;

// ============================================================================
// The fake broker
// ============================================================================

struct Part {
    std::vector<json> records;
    int64_t log_start = 0;
    int created = 0;
    std::string last_write_at;
};

class FakeBroker {
public:
    std::map<std::string, std::map<std::string, Part>> queues;
    size_t records_per_entry = 10000;
    std::vector<std::string> safe_times;
    std::optional<std::pair<std::string, std::string>> grow_after_fetch;
    std::optional<json> fetch_override;
    std::optional<json> changed_override;

private:
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    mutable std::mutex mutex_;
    std::vector<std::pair<std::string, json>> hits_;
    int created_ = 0;

public:
    FakeBroker() {
        server_.Post(".*", [this](const httplib::Request& req, httplib::Response& res) {
            std::lock_guard<std::mutex> lock(mutex_);
            json body = json::parse(req.body);
            hits_.emplace_back(req.path, body);
            json answer;
            if (req.path == "/api/v1/fetch") {
                answer = fetch_override ? *fetch_override : fetch(body);
                if (grow_after_fetch) {
                    write(grow_after_fetch->first, grow_after_fetch->second, {json("late")});
                }
            } else if (req.path == "/api/v1/partitions/changed") {
                answer = changed_override ? *changed_override : changed(body);
            } else {
                res.status = 404;
                res.set_content(R"({"error":"not found"})", "application/json");
                return;
            }
            res.status = 200;
            res.set_content(answer.dump(), "application/json");
        });
        port_ = server_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this]() { server_.listen_after_bind(); });
        server_.wait_until_ready();
    }

    ~FakeBroker() {
        server_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    void write(const std::string& queue, const std::string& partition, const std::vector<json>& payloads,
               const std::string& last_write_at = TS) {
        auto& q = queues[queue];
        if (!q.count(partition)) {
            Part p;
            p.created = created_++;
            p.last_write_at = last_write_at;
            q.emplace(partition, std::move(p));
        }
        Part& p = q[partition];
        for (const auto& payload : payloads) {
            int64_t offset = p.records.empty() ? p.log_start : p.records.back()["offset"].get<int64_t>() + 1;
            p.records.push_back({{"offset", offset},
                                 {"transactionId", partition + "-" + std::to_string(offset)},
                                 {"payload", payload},
                                 {"ts", TS}});
        }
        p.last_write_at = last_write_at;
    }

    void trim(const std::string& queue, const std::string& partition, int64_t offset) {
        Part& p = queues[queue][partition];
        p.records.erase(std::remove_if(p.records.begin(), p.records.end(),
                                       [&](const json& r) { return r["offset"].get<int64_t>() < offset; }),
                        p.records.end());
        p.log_start = offset;
    }

    std::vector<json> fetch_bodies() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<json> out;
        for (const auto& [path, body] : hits_) {
            if (path == "/api/v1/fetch") out.push_back(body);
        }
        return out;
    }

    std::vector<json> changed_bodies() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<json> out;
        for (const auto& [path, body] : hits_) {
            if (path == "/api/v1/partitions/changed") out.push_back(body);
        }
        return out;
    }

private:
    json fetch(const json& body) {
        const bool has_ceiling = body.contains("maxBytes");
        const int64_t ceiling = has_ceiling ? body["maxBytes"].get<int64_t>() : 0;
        int64_t used = 0;
        json entries = json::array();
        for (const auto& e : body["entries"]) {
            const std::string queue = e["queue"];
            const std::string partition = e.value("partition", std::string("Default"));
            json out = {{"queue", queue}, {"partition", partition}, {"records", json::array()},
                        {"highWatermark", 0}, {"logStartOffset", 0}};
            auto q = queues.find(queue);
            if (q == queues.end()) {
                out["error"] = "UNKNOWN_TOPIC_OR_PARTITION";
                entries.push_back(out);
                continue;
            }
            auto pi = q->second.find(partition);
            if (pi == q->second.end()) {
                entries.push_back(out);
                continue;
            }
            const Part& p = pi->second;
            const int64_t high = p.records.empty() ? p.log_start : p.records.back()["offset"].get<int64_t>() + 1;
            out["highWatermark"] = high;
            out["logStartOffset"] = p.log_start;
            const int64_t offset = e["offset"].get<int64_t>();
            if (offset < p.log_start || offset > high) {
                out["error"] = "OFFSET_OUT_OF_RANGE";
                entries.push_back(out);
                continue;
            }
            int64_t budget = e.value("maxBytes", MIB);
            if (has_ceiling) {
                if (used >= ceiling) {
                    entries.push_back(out);
                    continue;
                }
                budget = std::min(budget, ceiling - used);
            }
            int64_t spent = 0;
            for (const auto& r : p.records) {
                if (r["offset"].get<int64_t>() < offset) continue;
                if (out["records"].size() >= records_per_entry) break;
                const int64_t weight = std::max<int64_t>(static_cast<int64_t>(r["payload"].dump().size()), 1);
                if (!out["records"].empty() && spent + weight > budget) break;
                spent += weight;
                out["records"].push_back(r);
            }
            used += spent;
            entries.push_back(out);
        }
        return {{"entries", entries}};
    }

    json changed(const json& body) {
        std::string safe_time = "2026-10-20T12:00:00.000000Z";
        if (safe_times.size() > 1) {
            safe_time = safe_times.front();
            safe_times.erase(safe_times.begin());
        } else if (safe_times.size() == 1) {
            safe_time = safe_times.front();
        }
        json entries = json::array();
        for (const auto& e : body["entries"]) {
            const std::string queue = e["queue"];
            auto q = queues.find(queue);
            if (q == queues.end()) {
                entries.push_back({{"queue", queue}, {"error", "UNKNOWN_TOPIC_OR_PARTITION"}});
                continue;
            }
            int from = 0;
            if (e.contains("after")) {
                std::smatch m;
                const std::string after = e["after"];
                if (!std::regex_match(after, m, std::regex("p\\|(\\d+)"))) {
                    entries.push_back({{"queue", queue}, {"error", "BAD_CURSOR"}});
                    continue;
                }
                from = std::stoi(m[1]) + 1;
            }
            const size_t limit = static_cast<size_t>(std::clamp<int64_t>(e.value("limit", 1000), 1, 1000));
            std::vector<std::pair<std::string, const Part*>> all;
            for (const auto& [name, p] : q->second) all.emplace_back(name, &p);
            std::sort(all.begin(), all.end(), [](const auto& a, const auto& b) { return a.second->created < b.second->created; });
            json partitions = json::array();
            int last = 0;
            for (const auto& [name, p] : all) {
                if (p->created < from) continue;
                if (e.contains("since") && p->last_write_at < e["since"].get<std::string>()) continue;
                const int64_t last_offset = p->records.empty() ? p->log_start - 1 : p->records.back()["offset"].get<int64_t>();
                partitions.push_back({{"name", name}, {"id", "id-" + name}, {"lastOffset", last_offset},
                                      {"logStart", p->log_start}, {"lastWriteAt", p->last_write_at}});
                last = p->created;
                if (partitions.size() >= limit) break;
            }
            entries.push_back({{"queue", queue}, {"partitions", partitions},
                               {"next", partitions.size() >= limit ? json("p|" + std::to_string(last)) : json()}});
        }
        return {{"safeTime", safe_time}, {"safeTimeDegraded", false}, {"entries", entries}};
    }
};

// ============================================================================
// Assertions
// ============================================================================

static int failures = 0;
static std::string live_url;

void check(bool condition, const std::string& what) {
    if (!condition) {
        ++failures;
        std::cout << RED << "    x " << what << RESET << std::endl;
    }
}

template <typename Fn>
void check_throws(Fn fn, const std::string& contains, const std::string& what) {
    try {
        fn();
        check(false, what + " (nothing was thrown)");
    } catch (const std::exception& e) {
        check(contains.empty() || std::string(e.what()).find(contains) != std::string::npos,
              what + " (threw: " + e.what() + ")");
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

ClientConfig one_attempt() {
    ClientConfig config;
    config.retry_attempts = 1;
    return config;
}

std::vector<json> payloads(int n) {
    std::vector<json> out;
    for (int i = 0; i < n; ++i) out.push_back({{"n", i}});
    return out;
}

std::vector<int64_t> offsets_of(const FetchedPartition& p) {
    std::vector<int64_t> out;
    for (const auto& r : p.records) out.push_back(r.offset);
    return out;
}

std::vector<int64_t> first_offsets(const FakeBroker& broker) {
    std::vector<int64_t> out;
    for (const auto& b : broker.fetch_bodies()) out.push_back(b["entries"][0]["offset"].get<int64_t>());
    return out;
}

std::vector<int64_t> range(int64_t from, int64_t to) {
    std::vector<int64_t> out;
    for (int64_t i = from; i < to; ++i) out.push_back(i);
    return out;
}

// ============================================================================
// fetch
// ============================================================================

void test_fetch_reads_every_record_in_one_call_when_they_fit() {
    FakeBroker broker;
    broker.write("tf.events", "trace-1", payloads(3));
    broker.write("tf.events", "trace-2", {json{{"a", 1}}});
    QueenClient client({broker.url()}, one_attempt());

    auto read = client.queue("tf.events").fetch({"trace-1", "trace-2"});
    const auto& t1 = read["trace-1"];
    check(offsets_of(t1) == range(0, 3), "trace-1 offsets");
    check(t1.records[1].payload == json{{"n", 1}}, "payload");
    check(t1.records[0].transaction_id == "trace-1-0" && t1.records[0].ts == TS, "transaction id and ts");
    check(t1.partition == "trace-1" && t1.high_watermark == 3 && t1.log_start_offset == 0 && t1.next_offset == 3,
          "bounds");
    check(read["trace-2"].records.at(0).payload == json{{"a", 1}}, "trace-2");
    auto bodies = broker.fetch_bodies();
    check(bodies.size() == 1, "one call");
    json want = {{"entries", {{{"queue", "tf.events"}, {"partition", "trace-1"}, {"offset", 0}, {"maxBytes", 8 * MIB}},
                              {{"queue", "tf.events"}, {"partition", "trace-2"}, {"offset", 0}, {"maxBytes", 8 * MIB}}}},
                 {"maxBytes", 8 * MIB}};
    check(bodies.at(0) == want, "request body: " + bodies.at(0).dump());
}

void test_fetch_asks_again_until_the_high_watermark_of_the_first_answer() {
    FakeBroker broker;
    broker.records_per_entry = 3;
    broker.write("q", "p", payloads(10));
    QueenClient client({broker.url()}, one_attempt());
    auto read = client.queue("q").fetch({"p"});
    check(offsets_of(read["p"]) == range(0, 10), "offsets 0..9");
    check(first_offsets(broker) == std::vector<int64_t>{0, 3, 6, 9}, "asked from 0, 3, 6, 9");
    check(read["p"].next_offset == 10, "next offset");
}

void test_fetch_does_not_chase_a_partition_written_during_the_read() {
    FakeBroker broker;
    broker.records_per_entry = 2;
    broker.write("q", "p", payloads(4));
    broker.grow_after_fetch = std::make_pair(std::string("q"), std::string("p"));
    QueenClient client({broker.url()}, one_attempt());
    auto read = client.queue("q").fetch({"p"});
    check(offsets_of(read["p"]) == range(0, 4), "offsets 0..3");
    check(broker.fetch_bodies().size() == 2 && read["p"].next_offset == 4, "two calls, next 4");
}

void test_fetch_asks_for_at_most_1000_partitions_per_call() {
    FakeBroker broker;
    std::vector<std::string> names;
    for (int i = 0; i < 2500; ++i) {
        names.push_back("p-" + std::to_string(i));
        broker.write("q", names.back(), {json(names.back())});
    }
    QueenClient client({broker.url()}, one_attempt());
    auto read = client.queue("q").fetch(names);
    std::vector<size_t> sizes;
    for (const auto& b : broker.fetch_bodies()) sizes.push_back(b["entries"].size());
    check(sizes == std::vector<size_t>{1000, 1000, 500}, "1000, 1000, 500 entries per call");
    check(read.size() == 2500, "2500 partitions");
    bool all = true;
    for (const auto& name : names) all = all && read[name].records.at(0).payload == json(name);
    check(all, "every partition read");
}

void test_fetch_asks_again_for_the_partitions_the_call_ceiling_left_empty() {
    FakeBroker broker;
    const json fat = std::string(100, 'x');
    for (const char* name : {"a", "b", "c"}) broker.write("q", name, {fat, fat});
    QueenClient client({broker.url()}, one_attempt());
    FetchOptions options;
    options.max_bytes = 250;
    auto read = client.queue("q").fetch({"a", "b", "c"}, options);
    for (const char* name : {"a", "b", "c"}) {
        check(offsets_of(read[name]) == range(0, 2), std::string(name) + " offsets");
    }
    auto bodies = broker.fetch_bodies();
    check(bodies.size() > 1, "more than one call");
    bool ceiling = true;
    for (const auto& b : bodies) ceiling = ceiling && b["maxBytes"] == 250;
    check(ceiling, "every call carries the ceiling");
}

void test_fetch_reads_from_log_start_when_retention_deleted_the_start() {
    FakeBroker broker;
    broker.write("q", "p", payloads(10));
    broker.trim("q", "p", 6);
    QueenClient client({broker.url()}, one_attempt());
    auto read = client.queue("q").fetch({"p"});
    check(offsets_of(read["p"]) == range(6, 10), "offsets 6..9");
    check(read["p"].log_start_offset == 6, "log start");
    check(first_offsets(broker) == std::vector<int64_t>{0, 6}, "asked from 0 then 6");
}

void test_fetch_starts_each_partition_at_its_offset() {
    FakeBroker broker;
    broker.write("q", "a", payloads(5));
    broker.write("q", "b", payloads(5));
    QueenClient client({broker.url()}, one_attempt());
    FetchOptions options;
    options.offsets["a"] = 3;
    auto read = client.queue("q").fetch({"a", "b"}, options);
    check(offsets_of(read["a"]) == range(3, 5), "a from 3");
    check(offsets_of(read["b"]) == range(0, 5), "b from 0");
    FetchOptions again_options;
    again_options.offsets["a"] = read["a"].next_offset;
    auto again = client.queue("q").fetch({"a"}, again_options);
    check(again["a"].records.empty() && again["a"].next_offset == 5, "nothing new");
}

void test_fetch_answers_no_records_for_a_missing_queue_an_unwritten_partition_and_an_offset_past_the_end() {
    FakeBroker broker;
    broker.write("q", "p", payloads(2));
    QueenClient client({broker.url()}, one_attempt());
    check(client.queue("nope").fetch({"p"})["p"].records.empty(), "missing queue");
    FetchOptions options;
    options.offsets["p"] = 7;
    auto read = client.queue("q").fetch({"never", "p"}, options);
    check(read["never"].records.empty(), "unwritten partition");
    check(read["p"].records.empty() && read["p"].next_offset == 7 && read["p"].high_watermark == 2,
          "offset past the end");
}

void test_fetch_throws_on_an_error_marker_it_does_not_know() {
    FakeBroker broker;
    broker.fetch_override = json::parse(
        R"({"entries":[{"queue":"q","partition":"p","records":[],"highWatermark":1,"logStartOffset":0,"error":"SOMETHING_NEW"}]})");
    QueenClient client({broker.url()}, one_attempt());
    check_throws([&]() { client.queue("q").fetch({"p"}); }, "SOMETHING_NEW", "an unknown marker throws");
}

void test_fetch_stops_when_a_call_reads_nothing_although_the_partition_is_not_at_its_end() {
    FakeBroker broker;
    broker.fetch_override = json::parse(
        R"({"entries":[{"queue":"q","partition":"p","records":[],"highWatermark":5,"logStartOffset":0}]})");
    QueenClient client({broker.url()}, one_attempt());
    auto read = client.queue("q").fetch({"p"});
    check(read["p"].records.empty() && broker.fetch_bodies().size() == 1, "one call, no records");
}

void test_fetch_asks_once_for_a_name_given_twice_and_refuses_bad_input_before_any_call() {
    FakeBroker broker;
    broker.write("q", "p", payloads(1));
    QueenClient client({broker.url()}, one_attempt());
    check(client.queue("q").fetch({"p", "p"}).size() == 1, "one partition");
    check(broker.fetch_bodies().at(0)["entries"].size() == 1, "one entry");
    check_throws([&]() { client.queue("q").fetch({""}); }, "", "an empty name");
    FetchOptions negative;
    negative.offsets["p"] = -1;
    check_throws([&]() { client.queue("q").fetch({"p"}, negative); }, "", "a negative offset");
    FetchOptions zero;
    zero.max_bytes = 0;
    check_throws([&]() { client.queue("q").fetch({"p"}, zero); }, "", "max_bytes 0");
    check(broker.fetch_bodies().size() == 1, "no call for bad input");
}

// ============================================================================
// changed_since
// ============================================================================

void test_changed_since_follows_the_page_cursor_to_the_end_of_the_pass() {
    FakeBroker broker;
    for (int i = 0; i < 2300; ++i) broker.write("tf.events", "t-" + std::to_string(i), {json(i)});
    QueenClient client({broker.url()}, one_attempt());
    auto changed = client.queue("tf.events").changed_since("2026-10-20T09:00:00Z");
    check(changed.partitions.size() == 2300, "2300 partitions");
    const auto& p0 = changed.partitions.at(0);
    check(p0.name == "t-0" && p0.id == "id-t-0" && p0.last_offset == 0 && p0.log_start == 0 && p0.last_write_at == TS,
          "partition fields");
    check(changed.partitions.at(2299).name == "t-2299", "creation order");
    check(changed.safe_time == "2026-10-20T12:00:00.000000Z", "safe time");
    std::vector<json> afters;
    bool shape = true;
    for (const auto& b : broker.changed_bodies()) {
        const json& e = b["entries"][0];
        afters.push_back(e.value("after", json()));
        shape = shape && e["since"] == "2026-10-20T09:00:00Z" && e["limit"] == 1000 && e["queue"] == "tf.events";
    }
    check(afters == std::vector<json>{json(), json("p|999"), json("p|1999")}, "cursors");
    check(shape, "every page carries queue, since, limit");
}

void test_changed_since_lists_only_what_was_written_at_or_after_since() {
    FakeBroker broker;
    broker.write("q", "old", {json(1)}, "2026-10-20T08:00:00.000000Z");
    broker.write("q", "new", {json(1)}, "2026-10-20T10:30:00.000000Z");
    QueenClient client({broker.url()}, one_attempt());
    auto since = std::chrono::system_clock::time_point(std::chrono::seconds(1792490400));  // 2026-10-20T10:00:00Z
    auto changed = client.queue("q").changed_since(since);
    check(changed.partitions.size() == 1 && changed.partitions.at(0).name == "new", "only new");
    check(broker.changed_bodies().at(0)["entries"][0]["since"] == "2026-10-20T10:00:00.000000Z", "since spelling");
    check(reads::iso_from_micros(1792506600123456LL) == "2026-10-20T14:30:00.123456Z", "microseconds");
}

void test_changed_since_without_since_lists_every_partition_and_sends_no_since() {
    FakeBroker broker;
    broker.write("q", "a", {json(1)}, "2020-01-01T00:00:00.000000Z");
    QueenClient client({broker.url()}, one_attempt());
    auto changed = client.queue("q").changed_since();
    check(changed.partitions.size() == 1, "one partition");
    check(!broker.changed_bodies().at(0)["entries"][0].contains("since"), "no since sent");
}

void test_changed_since_answers_the_smallest_safe_time_of_its_pages() {
    FakeBroker broker;
    for (int i = 0; i < 1500; ++i) broker.write("q", "p-" + std::to_string(i), {json(1)});
    broker.safe_times = {"2026-10-20T12:00:05.000000Z", "2026-10-20T12:00:01.000000Z"};
    QueenClient client({broker.url()}, one_attempt());
    check(client.queue("q").changed_since().safe_time == "2026-10-20T12:00:01.000000Z", "smallest safe time");
}

void test_changed_since_answers_nothing_for_a_missing_queue_and_throws_on_bad_cursor() {
    FakeBroker broker;
    QueenClient client({broker.url()}, one_attempt());
    auto changed = client.queue("ghost").changed_since("2026-10-20T00:00:00Z");
    check(changed.partitions.empty() && changed.safe_time == "2026-10-20T12:00:00.000000Z", "missing queue");
    FakeBroker bad;
    bad.changed_override = json::parse(
        R"({"safeTime":"2026-10-20T12:00:00.000000Z","safeTimeDegraded":false,"entries":[{"queue":"q","error":"BAD_CURSOR"}]})");
    QueenClient bad_client({bad.url()}, one_attempt());
    check_throws([&]() { bad_client.queue("q").changed_since(); }, "BAD_CURSOR", "BAD_CURSOR throws");
}

// ============================================================================
// Live
// ============================================================================

std::string run_suffix() {
    std::random_device rd;
    return std::to_string(rd());
}

void live_fetch_reads_every_record_by_offset() {
    QueenClient client(live_url);
    const std::string queue = "cpp-reads-fetch-" + run_suffix();
    std::vector<std::string> partitions;
    for (int i = 0; i < 1200; ++i) partitions.push_back("entity-" + std::to_string(i));
    const int per_partition = 3;
    for (int n = 0; n < per_partition; ++n) {
        for (const auto& p : partitions) {
            client.queue(queue).partition(p).push(std::vector<json>{
                json{{"transactionId", p + "-" + std::to_string(n)}, {"data", json{{"p", p}, {"n", n}}}}});
        }
    }
    auto read = client.queue(queue).fetch(partitions);
    check(read.size() == partitions.size(), "every partition answered");
    bool exact = true;
    for (const auto& p : partitions) {
        const auto& part = read[p];
        exact = exact && part.records.size() == static_cast<size_t>(per_partition) && part.high_watermark == per_partition &&
                part.next_offset == per_partition && part.log_start_offset == 0;
        for (int n = 0; exact && n < per_partition; ++n) {
            const auto& r = part.records[n];
            exact = r.offset == n && r.payload == json{{"p", p}, {"n", n}} && r.transaction_id == p + "-" + std::to_string(n);
        }
    }
    check(exact, "every record, in order, with its offset");

    client.queue(queue).partition("entity-7").push(std::vector<json>{json{{"data", json{{"late", true}}}}});
    FetchOptions options;
    options.offsets["entity-7"] = read["entity-7"].next_offset;
    options.offsets["entity-8"] = read["entity-8"].next_offset;
    auto later = client.queue(queue).fetch({"entity-7", "entity-8"}, options);
    check(later["entity-7"].records.size() == 1 && later["entity-7"].records[0].offset == per_partition &&
              later["entity-7"].records[0].payload == json{{"late", true}},
          "a read from next_offset answers only what came after");
    check(later["entity-8"].records.empty(), "nothing new");

    auto popped = client.queue(queue).partition("entity-9").batch(10).wait(false).pop();
    check(popped.size() == static_cast<size_t>(per_partition), "a pop after the fetch gets the messages");
    check(client.queue(queue + "-does-not-exist").fetch({"x"})["x"].records.empty(), "missing queue");
}

void live_changed_since_lists_the_written_partitions() {
    QueenClient client(live_url);
    const std::string queue = "cpp-reads-changed-" + run_suffix();
    auto before = std::chrono::system_clock::now() - std::chrono::minutes(1);
    std::vector<std::string> names;
    for (int i = 0; i < 1100; ++i) names.push_back("c-" + std::to_string(i));
    for (const auto& p : names) client.queue(queue).partition(p).push(std::vector<json>{json{{"data", json{{"p", p}}}}});

    auto first = client.queue(queue).changed_since(before);
    std::vector<std::string> listed;
    for (const auto& p : first.partitions) listed.push_back(p.name);
    std::sort(listed.begin(), listed.end());
    auto want = names;
    std::sort(want.begin(), want.end());
    check(listed == want, "every written partition, over two pages");
    check(!first.safe_time.empty(), "a safe time");

    client.queue(queue).partition("c-42").push(std::vector<json>{json{{"data", json{{"again", true}}}}});
    auto next = client.queue(queue).changed_since(first.safe_time);
    bool found = false, none_before = true;
    for (const auto& p : next.partitions) {
        found = found || (p.name == "c-42" && p.last_offset == 1);
        none_before = none_before && p.last_write_at >= first.safe_time;
    }
    check(found, "c-42 listed after its write");
    check(none_before, "nothing written before safe_time listed");
    check(client.queue(queue).changed_since().partitions.size() == names.size(), "every partition without since");
    check(client.queue(queue + "-does-not-exist").changed_since(before).partitions.empty(), "missing queue");
}

int main(int argc, char** argv) {
    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - Reads by Offset Tests" << std::endl;
    std::cout << "========================================\n" << std::endl;

    run_test("fetch reads every record in one call when they fit", test_fetch_reads_every_record_in_one_call_when_they_fit);
    run_test("fetch asks again until the high watermark of the first answer",
             test_fetch_asks_again_until_the_high_watermark_of_the_first_answer);
    run_test("fetch does not chase a partition written during the read",
             test_fetch_does_not_chase_a_partition_written_during_the_read);
    run_test("fetch asks for at most 1000 partitions per call", test_fetch_asks_for_at_most_1000_partitions_per_call);
    run_test("fetch asks again for the partitions the call ceiling left empty",
             test_fetch_asks_again_for_the_partitions_the_call_ceiling_left_empty);
    run_test("fetch reads from log start when retention deleted the start",
             test_fetch_reads_from_log_start_when_retention_deleted_the_start);
    run_test("fetch starts each partition at its offset", test_fetch_starts_each_partition_at_its_offset);
    run_test("fetch answers no records for a missing queue, an unwritten partition and an offset past the end",
             test_fetch_answers_no_records_for_a_missing_queue_an_unwritten_partition_and_an_offset_past_the_end);
    run_test("fetch throws on an error marker it does not know", test_fetch_throws_on_an_error_marker_it_does_not_know);
    run_test("fetch stops when a call reads nothing although the partition is not at its end",
             test_fetch_stops_when_a_call_reads_nothing_although_the_partition_is_not_at_its_end);
    run_test("fetch asks once for a name given twice and refuses bad input before any call",
             test_fetch_asks_once_for_a_name_given_twice_and_refuses_bad_input_before_any_call);
    run_test("changed_since follows the page cursor to the end of the pass",
             test_changed_since_follows_the_page_cursor_to_the_end_of_the_pass);
    run_test("changed_since lists only what was written at or after since",
             test_changed_since_lists_only_what_was_written_at_or_after_since);
    run_test("changed_since without since lists every partition and sends no since",
             test_changed_since_without_since_lists_every_partition_and_sends_no_since);
    run_test("changed_since answers the smallest safe time of its pages",
             test_changed_since_answers_the_smallest_safe_time_of_its_pages);
    run_test("changed_since answers nothing for a missing queue and throws on BAD_CURSOR",
             test_changed_since_answers_nothing_for_a_missing_queue_and_throws_on_bad_cursor);

    if (argc > 1) {
        live_url = argv[1];
        std::cout << "\n--- live broker: " << live_url << " ---\n" << std::endl;
        run_test("live: fetch reads every record by offset", live_fetch_reads_every_record_by_offset);
        run_test("live: changed_since lists the written partitions", live_changed_since_lists_the_written_partitions);
    } else {
        std::cout << "\n(no server URL given: the live half was not run)" << std::endl;
    }

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All reads tests passed" << RESET << std::endl;
        return 0;
    }
    std::cout << RED << failures << " reads check(s) failed" << RESET << std::endl;
    return 1;
}
