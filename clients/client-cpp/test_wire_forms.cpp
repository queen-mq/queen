/**
 * Queen C++ Client - binary push body and framed pop answer, wire tests
 *
 * WHAT THIS FILE IS FOR.
 * ClientConfig has three options for a broker that has the binary push body
 * and the framed pop answer: binary_push, push_compression and framed_pop. All
 * three are off by default. What is at stake:
 *
 *   1. THE DEFAULT. A client that sets none of them must send the JSON requests
 *      every released broker knows.
 *   2. THE BYTES. The binary push body and its framed form are read here field
 *      by field, as the broker reads them.
 *   3. THE SAME MESSAGES. A consumer that sets framed_pop must be handed
 *      exactly the messages the JSON answer gives it, with the answer's own
 *      keys (the conflation echo, the autopilot's choice) where they always
 *      were.
 *   4. THE ORDER OF A TRANSACTION'S RESULTS. With binary_push and
 *      push_compression a bundle that pushes goes out as the binary
 *      transaction body, and the broker lists the head's acks before the
 *      pushes: the caller must get the results back in the order of its own
 *      operations.
 *   5. THE TRANSPORT UNDER ALL OF IT (2.7.1). Sequential requests share one
 *      pooled keep-alive connection, and every request names its Content-Type
 *      exactly once (2.7.0 sent a POST's twice).
 *
 * push_compression and framed_pop need zstd: this file builds without it too
 * (the Makefile adds -DQUEEN_CLIENT_ZSTD when the library is installed) and
 * then skips the cases that need it.
 *
 * Like its siblings this runs against an in-process httplib::Server -- no
 * broker:
 *
 *   make wireforms && ./bin/test_wire_forms
 *
 * Siblings: test_autopilot.cpp, test_conflation.cpp, test_buffer.cpp.
 */

#include "queen_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <vector>
#include <set>
#include <string>
#include <functional>

using namespace queen;
using json = nlohmann::json;

#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define YELLOW "\033[33m"
#define RESET "\033[0m"

static const std::string TXN = "0190aaaa-0000-7000-8000-000000000001";
static const std::string TRACE = "0190aaaa-0000-4000-8000-000000000001";

// The JSON answer of two messages of partition 7 ("p7") of "orders", as the
// broker renders it.
static const std::string TWO_MESSAGES_JSON =
    R"({"success":true,"queue":"orders","partition":"p7","partitionId":"7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","messages":[)"
    R"({"id":"01000000-0000-7000-8000-000000000000","transactionId":"t-10","traceId":null,"data":{"n":10},"producerSub":null,"createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":10},)"
    R"({"id":"02000000-0000-7000-8000-000000000000","transactionId":"t-11","traceId":null,"data":{"n":11,"s":"x"},"producerSub":"svc","createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":11})"
    R"(],"partitionsClaimed":1})";

// ============================================================================
// Test double
// ============================================================================

struct RecordedCall {
    std::string method;
    std::string target;        // raw request target, query string included
    std::string content_type;
    size_t content_type_count = 0;  // copies of the Content-Type header
    std::string body;          // as bytes: a binary push body is not text
    bool asked_frames = false; // the pop carried frames=true
    int remote_port = -1;      // the client's end of the connection it came on
};

using Responder = std::function<void(const httplib::Request&, httplib::Response&)>;

inline Responder answer(const std::string& body, const std::string& content_type = "application/json") {
    return [body, content_type](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content(body, content_type);
    };
}

inline Responder queued(size_t count = 1) {
    json items = json::array();
    for (size_t i = 0; i < count; ++i) items.push_back({{"status", "queued"}});
    return answer(items.dump());
}

inline Responder framed(const std::string& body) {
    return answer(body, wire::POP_FRAMES_CONTENT_TYPE);
}

inline Responder nothing() {
    return [](const httplib::Request&, httplib::Response& res) { res.status = 204; };
}

class StubServer {
private:
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    Responder responder_;

    mutable std::mutex mutex_;
    std::vector<RecordedCall> calls_;

    std::vector<RecordedCall> matching(const std::string& method, const std::string& prefix) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<RecordedCall> out;
        for (const auto& call : calls_) {
            if (call.method == method && call.target.rfind(prefix, 0) == 0) out.push_back(call);
        }
        return out;
    }

public:
    explicit StubServer(Responder responder) : responder_(std::move(responder)) {
        auto handler = [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                calls_.push_back({req.method, req.target, req.get_header_value("Content-Type"),
                                  req.get_header_value_count("Content-Type"), req.body,
                                  req.get_param_value("frames") == "true", req.remote_port});
            }
            responder_(req, res);
        };

        server_.Get(".*", handler);
        server_.Post(".*", handler);
        server_.Put(".*", handler);
        server_.Delete(".*", handler);

        port_ = server_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this]() { server_.listen_after_bind(); });
        server_.wait_until_ready();
    }

    ~StubServer() {
        server_.stop();
        if (thread_.joinable()) thread_.join();
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    std::vector<RecordedCall> calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_;
    }

    std::vector<RecordedCall> pushes() const { return matching("POST", "/api/v1/push"); }
    std::vector<RecordedCall> transactions() const { return matching("POST", "/api/v1/transaction"); }
    std::vector<RecordedCall> pops() const { return matching("GET", "/api/v1/pop"); }
};

/// Cancels a consume loop after a budget, so a loop that fails to stop on its
/// own FAILS its test instead of hanging it. Hand token() to the builder's
/// cancel() before consume().
class StopAfter {
private:
    Cancel token_;
    std::atomic<bool> done_{false};
    std::thread timer_;

public:
    explicit StopAfter(int budget_millis) {
        timer_ = std::thread([this, budget_millis]() {
            auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(budget_millis);
            while (!done_.load() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            token_.cancel();
        });
    }

    ~StopAfter() {
        done_.store(true);
        if (timer_.joinable()) timer_.join();
    }

    const Cancel& token() const { return token_; }
};

// ============================================================================
// Assertions
// ============================================================================

static int failures = 0;
static int skipped = 0;

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

/// A test of push_compression or framed_pop: run where the build has zstd.
void run_zstd_test(const std::string& name, std::function<void()> test_fn) {
    if (wire::zstd_available()) {
        run_test(name, std::move(test_fn));
        return;
    }
    ++skipped;
    std::cout << YELLOW << "SKIP: " << name << " (built without zstd)" << RESET << std::endl;
}

ClientConfig config_with(bool binary_push, bool push_compression, bool framed_pop) {
    ClientConfig config;
    config.retry_attempts = 1;
    config.binary_push = binary_push;
    config.push_compression = push_compression;
    config.framed_pop = framed_pop;
    return config;
}

/// A request's Content-Type, and that it travelled ONCE: 2.7.0 put a POST's in
/// its own headers and httplib added a second copy.
void check_content_type(const RecordedCall& call, const std::string& want) {
    const std::string where = call.method + " " + call.target.substr(0, call.target.find('?'));
    check(call.content_type == want, where + ": content type " + call.content_type + ", want " + want);
    check(call.content_type_count == 1,
          where + ": Content-Type sent " + std::to_string(call.content_type_count) + " times");
}

/// What a push answered, for a failure message.
std::string statuses(const std::vector<PushResult>& results) {
    std::string out = "[";
    for (const auto& r : results) out += (out.size() > 1 ? "," : "") + r.status_text;
    return out + "]";
}

/// Every field of each message, in order: all a consumer can tell apart.
json fields_of(const std::vector<Message>& messages) {
    json out = json::array();
    for (const auto& m : messages) out.push_back(m.to_json());
    return out;
}

// ============================================================================
// Reading a binary push body
// ============================================================================

/// Reads the fields of a binary push body in order.
struct Reader {
    std::string data;
    size_t at = 0;

    explicit Reader(std::string bytes) : data(std::move(bytes)) {}

    std::string take(size_t n) {
        if (at + n > data.size()) throw std::runtime_error("a binary push body ends early");
        at += n;
        return data.substr(at - n, n);
    }

    size_t number(size_t width) {
        const std::string b = take(width);
        size_t value = 0;
        for (size_t i = 0; i < width; ++i) value |= static_cast<size_t>(static_cast<unsigned char>(b[i])) << (8 * i);
        return value;
    }

    size_t u16() { return number(2); }
    size_t u32() { return number(4); }
    std::string text16() { return take(u16()); }
    size_t left() const { return data.size() - at; }
};

struct PlainGroup {
    std::string queue;
    std::string partition;
    std::vector<std::pair<std::string, std::string>> items;  // (transaction id, payload)
};

/// The groups of a plain binary push body (after magic and flags), every item
/// with its transaction id.
std::vector<PlainGroup> plain_groups(const std::string& bytes) {
    Reader r(bytes);
    std::vector<PlainGroup> groups;
    for (size_t g = r.u32(); g > 0; --g) {
        PlainGroup group;
        group.queue = r.text16();
        group.partition = r.text16();
        for (size_t n = r.u32(); n > 0; --n) {
            std::string txn = r.text16();
            group.items.emplace_back(std::move(txn), r.take(r.u32()));
        }
        groups.push_back(std::move(group));
    }
    check(r.left() == 0, "bytes left after the last payload");
    return groups;
}

struct StoredFrame {
    int flags = 0;
    std::string id;
    std::string txn;
    std::string payload;
};

struct FramedGroup {
    std::string queue;
    std::string partition;
    size_t count = 0;
    std::vector<StoredFrame> frames;
};

/// The groups of a framed binary push body, each with its frames unpacked.
std::vector<FramedGroup> framed_groups(const std::string& bytes) {
    Reader r(bytes);
    std::vector<FramedGroup> groups;
    for (size_t g = r.u32(); g > 0; --g) {
        FramedGroup group;
        group.queue = r.text16();
        group.partition = r.text16();
        group.count = r.u32();
        Reader f(wire::zstd_decompress(r.take(r.u32())));
        while (f.left() > 0) {
            Reader body(f.take(f.u32()));
            StoredFrame frame;
            frame.flags = static_cast<unsigned char>(body.take(1)[0]);
            frame.id = body.take(16);
            frame.txn = body.text16();
            frame.payload = body.take(body.left());
            group.frames.push_back(std::move(frame));
        }
        groups.push_back(std::move(group));
    }
    check(r.left() == 0, "bytes left after the last group");
    return groups;
}

// ============================================================================
// Push
// ============================================================================

void test_a_push_is_json_by_default() {
    StubServer server(queued());
    QueenClient client({server.url()}, config_with(false, false, false));

    // 2.7.1: push() takes the payload itself, sent as it is.
    std::vector<PushResult> result = client.queue("orders").partition("p1").push(json{{"a", 1}});

    check(result.size() == 1 && result[0].status == PushStatus::Queued && result[0].status_text == "queued",
          "the push answered " + statuses(result));
    const auto sent = server.pushes();
    check(sent.size() == 1, "one push request");
    check_content_type(sent[0], "application/json");
    json body = json::parse(sent[0].body);
    check(body.size() == 1 && body.contains("items"), "the body is {items}");
    check(body["items"][0]["payload"] == json({{"a", 1}}), "the payload travels as JSON");
    // An item is its queue, partition, payload and transaction id, and nothing
    // else: since 2.7.1 a plain push carries no trace id.
    const json& item = body["items"][0];
    check(item.size() == 4 && item["queue"] == "orders" && item["partition"] == "p1" &&
              item["transactionId"].is_string() && item["transactionId"].get<std::string>().size() == 36,
          "the item: " + item.dump());
}

void test_binary_push_sends_the_binary_body_with_each_transaction_id() {
    StubServer server(queued());
    QueenClient client({server.url()}, config_with(true, false, false));

    std::vector<PushResult> result = client.queue("orders").partition("p1")
        .push_items({PushItem{json{{"a", 1}}, TXN}});

    check(result.size() == 1 && result[0].status == PushStatus::Queued, "the push answered " + statuses(result));
    const auto sent = server.pushes();
    check(sent.size() == 1, "one push request");
    check_content_type(sent[0], "application/x-queen-push");
    check(sent[0].body.substr(0, 5) == std::string("QPB1\x04", 5), "a plain binary push body with transaction ids");
    const auto groups = plain_groups(sent[0].body.substr(5));
    check(groups.size() == 1 && groups[0].queue == "orders" && groups[0].partition == "p1",
          "one group for orders/p1");
    check(groups[0].items.size() == 1 && groups[0].items[0].first == TXN &&
              groups[0].items[0].second == R"({"a":1})",
          "the item with its transaction id and payload");
}

void test_consecutive_items_of_one_partition_share_a_group() {
    auto item = [](const std::string& partition, int payload) {
        return json{{"queue", "q"}, {"partition", partition}, {"payload", payload},
                    {"transactionId", "t" + std::to_string(payload)}};
    };
    const auto bytes = wire::binary_push_body(
        json::array({item("a", 1), item("a", 2), item("b", 3), item("a", 4)}), false);

    check(bytes.has_value() && bytes->substr(0, 5) == std::string("QPB1\x04", 5), "a plain binary push body");
    const auto groups = plain_groups(bytes->substr(5));
    check(groups.size() == 3, "three groups, in the order of the items");
    check(groups[0].partition == "a" && groups[0].items.size() == 2 &&
              groups[0].items[0].first == "t1" && groups[0].items[1].second == "2",
          "a: t1, t2");
    check(groups[1].partition == "b" && groups[1].items.size() == 1 && groups[1].items[0].first == "t3", "b: t3");
    check(groups[2].partition == "a" && groups[2].items.size() == 1 && groups[2].items[0].first == "t4", "a: t4");
}

void test_binary_push_carries_payloads_that_are_not_objects_and_non_ascii_text() {
    StubServer server(queued(5));
    QueenClient client({server.url()}, config_with(true, false, false));
    const std::vector<json> payloads = {
        "testo \"con\" virgolette\n", json::array({1, 2}), 7, nullptr, json{{"è", "€"}}};

    // One message per payload: the array is one message's payload here.
    client.queue("ordini").partition("città").push_many(payloads);

    const auto groups = plain_groups(server.pushes()[0].body.substr(5));
    check(groups.size() == 1 && groups[0].partition == "città", "one group, its name in UTF-8");
    check(groups[0].items.size() == payloads.size(), "every payload travelled");
    for (size_t i = 0; i < payloads.size() && i < groups[0].items.size(); ++i) {
        check(json::parse(groups[0].items[i].second) == payloads[i], "payload " + std::to_string(i));
        check(groups[0].items[i].first.size() == 36, "the client mints a transaction id");
    }
}

/// Since 2.7.1 a plain push carries no trace id (the broker's push path cannot
/// store one; a transaction's push can, see below). So a plain push is never
/// held back to JSON by one, while the binary body still refuses an item that
/// names one -- the rule a transaction's push relies on.
void test_a_plain_push_carries_no_trace_id() {
    StubServer server(queued(2));
    QueenClient client({server.url()}, config_with(true, false, false));

    client.queue("orders").push_many({json{{"a", 1}}, json{{"a", 2}}});

    const auto sent = server.pushes();
    check(sent.size() == 1, "one push request");
    check_content_type(sent[0], "application/x-queen-push");
    check(sent[0].body.substr(0, 5) == std::string("QPB1\x04", 5), "the binary body, with transaction ids");
    const auto groups = plain_groups(sent[0].body.substr(5));
    check(groups.size() == 1 && groups[0].partition == "Default" && groups[0].items.size() == 2,
          "one group of two for orders/Default");
    check(!wire::binary_push_body(
              json::array({{{"queue", "q"}, {"partition", "p"}, {"payload", 1}, {"traceId", TRACE}}}), false),
          "an item that names a trace id is left to the JSON body");
}

void test_an_item_the_binary_body_does_not_carry_leaves_the_push_to_json() {
    // The binary body is not built; the JSON request then answers as it always did.
    check(!wire::binary_push_body(json::array({{{"queue", "q"}, {"partition", std::string(70000, 'p')}, {"payload", 1}}}), false),
          "a name over 65535 bytes");
    check(!wire::binary_push_body(json::array({{{"queue", "q"}, {"partition", "p"}, {"payload", 1}, {"transactionId", 5}}}), false),
          "a transaction id that is not a string");
    check(!wire::binary_push_body(json::array({{{"queue", "q"}, {"partition", "p"}}}), false), "no payload");
    check(!wire::binary_push_body(json::array({{{"queue", "q"}, {"partition", "p"}, {"payload", std::string("\xff\xfe")}}}), false),
          "a payload that is not UTF-8");
    check(!wire::binary_push_body(json::array(), false), "no items");
}

void test_a_buffered_push_is_binary_too() {
    StubServer server(queued());
    QueenClient client({server.url()}, config_with(true, false, false));
    BufferOptions options;
    options.message_count = 10;
    options.time_millis = 60000;

    const auto result = client.queue("orders").partition("p1").buffer(options).push(json{{"a", 1}});
    check(result.empty(), "a buffered push has no verdict yet: it comes with the flush");
    client.flush_all_buffers();

    const auto sent = server.pushes();
    check(sent.size() == 1, "one push request");
    check_content_type(sent[0], "application/x-queen-push");
    check(sent[0].body.substr(0, 5) == std::string("QPB1\x04", 5), "a plain binary push body");
    check(plain_groups(sent[0].body.substr(5))[0].items[0].second == R"({"a":1})", "the payload");
}

void test_the_counts_flag_and_mismatched_transaction_ids() {
    const std::vector<wire::PushBatch> batches = {{"q", "p", {"1"}, std::nullopt}};
    check(wire::encode_push_binary(batches, false, true).substr(0, 5) == std::string("QPB1\x02", 5), "flag 2 asks for the counts");
    check(wire::encode_push_binary(batches).substr(0, 5) == std::string("QPB1\x00", 5), "no flags by default");

    bool refused = false;
    try {
        wire::encode_push_binary({{"q", "p", {"1", "2"}, std::vector<std::string>{"t"}}});
    } catch (const std::invalid_argument& e) {
        refused = std::string(e.what()) == "1 transaction ids for 2 payloads";
    }
    check(refused, "transaction ids must match the payloads");
}

void test_the_zstd_flag_compresses_the_whole_body() {
    const std::vector<wire::PushBatch> batches = {{"q", "p", {"1"}, std::nullopt}};
    const std::string plain = wire::encode_push_binary(batches);
    const std::string packed = wire::encode_push_binary(batches, true);

    check(packed.substr(0, 5) == std::string("QPB1\x01", 5), "flag 1 says the body is one zstd frame");
    check(Reader(packed.substr(5, 4)).u32() == plain.size() - 5, "its raw length in front");
    check(wire::zstd_decompress(packed.substr(9)) == plain.substr(5), "the frame is the plain body");
    check(static_cast<unsigned char>(wire::encode_push_frames(batches, true)[4]) == (8 | 2), "frames with the counts answer");
}

void test_push_compression_sends_each_partition_as_one_zstd_frame_of_stored_frames() {
    StubServer server(queued(3));
    QueenClient client({server.url()}, config_with(true, true, false));

    client.queue("orders").partition("p1").push_items({
        PushItem{json{{"a", 1}}, TXN},
        PushItem{json{{"a", 2}}, ""},
        PushItem{"x", ""}});

    const auto sent = server.pushes();
    check_content_type(sent[0], "application/x-queen-push");
    check(sent[0].body.substr(0, 5) == std::string("QPB1\x08", 5), "a framed binary push body");
    const auto groups = framed_groups(sent[0].body.substr(5));
    check(groups.size() == 1 && groups[0].queue == "orders" && groups[0].partition == "p1" && groups[0].count == 3,
          "one group of three for orders/p1");
    const auto& frames = groups[0].frames;
    check(frames.size() == 3, "three frames");
    if (frames.size() != 3) return;
    check(frames[0].payload == R"({"a":1})" && frames[1].payload == R"({"a":2})" && frames[2].payload == R"("x")",
          "the payloads, in order");
    check(frames[0].txn == TXN, "a message keeps its own transaction id");
    std::set<std::string> ids;
    for (const auto& frame : frames) {
        check(frame.flags == 0, "no frame flags");
        check((static_cast<unsigned char>(frame.id[6]) >> 4) == 7, "the message id is a version-7 UUID");
        check((static_cast<unsigned char>(frame.id[8]) & 0xC0) == 0x80, "with the RFC variant");
        check(frame.txn.size() == 36, "a transaction id of 36 characters");
        ids.insert(frame.id);
    }
    check(ids.size() == 3, "three distinct message ids");
}

void test_a_frame_without_a_transaction_id_gets_the_text_of_its_id() {
    const std::string bytes = wire::encode_push_frames({{"q", "p", {R"({"a":1})"}, std::nullopt}});
    const auto frame = framed_groups(bytes.substr(5))[0].frames[0];
    check(frame.txn == util::uuid_text(reinterpret_cast<const uint8_t*>(frame.id.data())),
          "the transaction id is the text of the message id");

    // The broker asks for the content size of a frame it stores as it came.
    Reader r(bytes.substr(5));
    r.u32(); r.text16(); r.text16(); r.u32();
    const std::string zstd = r.take(r.u32());
    check(Reader(zstd.substr(0, 4)).u32() == 0xFD2FB528, "a zstd frame");
    // Frame header descriptor: the single-segment bit, or a content-size field
    // of 2 bytes or more, says the frame carries its content size.
    const int descriptor = static_cast<unsigned char>(zstd[4]);
    check((descriptor & 0x20) != 0 || (descriptor >> 6) != 0, "the frame carries its content size");
}

void test_the_zstd_options_say_what_they_need() {
    StubServer server(queued());
    // What building a client with these options throws, "" when it builds. 2.7.1
    // throws std::invalid_argument, its type for a misuse refused before
    // anything is sent (2.7.0 threw std::runtime_error).
    auto build = [&server](bool binary_push, bool push_compression, bool framed_pop) {
        try {
            QueenClient client({server.url()}, config_with(binary_push, push_compression, framed_pop));
            return std::string();
        } catch (const std::invalid_argument& e) {
            return std::string(e.what());
        } catch (const std::exception&) {
            return std::string("<not std::invalid_argument>");
        }
    };
    // push_compression alone compresses nothing: it is binary_push that sends frames.
    check(build(false, true, false).empty(), "push_compression without binary_push builds anywhere");
    check(build(true, false, false).empty(), "binary_push needs no zstd");
    if (wire::zstd_available()) {
        check(build(true, true, true).empty(), "every option builds with zstd");
    } else {
        check(build(true, true, false).find("QUEEN_CLIENT_ZSTD") != std::string::npos,
              "push_compression says it needs zstd");
        check(build(false, false, true).find("QUEEN_CLIENT_ZSTD") != std::string::npos,
              "framed_pop says it needs zstd");
    }
}

// ============================================================================
// Building a framed pop answer
// ============================================================================

std::string le(uint64_t value, size_t width) {
    std::string out;
    for (size_t i = 0; i < width; ++i) out.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
    return out;
}

std::string text(const std::string& s) { return le(s.size(), 2) + s; }

std::string from_hex(const std::string& uuid) {
    std::string out;
    std::string digits;
    for (char c : uuid) if (c != '-') digits.push_back(c);
    for (size_t i = 0; i + 1 < digits.size(); i += 2) {
        out.push_back(static_cast<char>(std::stoi(digits.substr(i, 2), nullptr, 16)));
    }
    return out;
}

/// One stored frame: u32 body_len | flags | id | [trace] | txn | [producer] | payload.
std::string stored_frame(int id, const std::string& txn, const std::string& payload,
                         const std::string& producer = "", const std::string& trace = "") {
    std::string id_bytes(16, '\0');
    id_bytes[0] = static_cast<char>(id);
    id_bytes[6] = 0x70;
    id_bytes[8] = static_cast<char>(0x80);
    std::string body;
    body.push_back(static_cast<char>((trace.empty() ? 0 : 1) | (producer.empty() ? 0 : 2)));
    body += id_bytes;
    if (!trace.empty()) body += from_hex(trace);
    body += text(txn);
    if (!producer.empty()) body += text(producer);
    body += payload;
    return le(body.size(), 4) + body;
}

/// A framed pop answer of one claim on partition 7 ("p7") of "orders", lease
/// "lease-1": one record at base offset 10 holding `frames`, created at
/// 2023-11-14T22:13:20Z, of which the claim covers start..=end.
std::string framed_answer(const std::string& own, uint64_t start, uint64_t end, bool compress,
                          const std::vector<std::string>& frames) {
    std::string raw;
    for (const auto& frame : frames) raw += frame;
    if (compress) raw = wire::zstd_compress(raw);
    std::string b("QPF1\x01", 5);
    b += le(own.size(), 4) + own;
    b += text("orders") + text("lease-1") + text("__QUEUE_MODE__");
    b += le(1, 4);                                    // claims
    b += le(7, 8) + text("p7") + le(start, 8) + le(end, 8);
    b += le(3, 4);                                    // delivery attempt
    b += le(1, 4);                                    // records
    b += le(10, 8) + le(1700000000000000ULL, 8) + le(frames.size(), 4);
    b.push_back(static_cast<char>(compress ? 1 : 0));
    b += le(raw.size(), 4) + raw;
    return b;
}

std::vector<std::string> two_frames() {
    return {stored_frame(1, "t-10", R"({"n":10})"),
            stored_frame(2, "t-11", R"({"n":11,"s":"x"})", "svc")};
}

/// The codecs a decoder case runs on: raw always, zstd where the build has it.
std::vector<bool> codecs() {
    return wire::zstd_available() ? std::vector<bool>{false, true} : std::vector<bool>{false};
}

/// What decode_pop_frames throws for `bytes`, or "" when it decodes. 2.7.1
/// throws queen::Error (a std::runtime_error), the client's base type; any
/// other exception escapes and fails the case.
std::string decode_error(const std::string& bytes) {
    try {
        wire::decode_pop_frames(bytes);
        return "";
    } catch (const Error& e) {
        return e.what();
    }
}

// ============================================================================
// The decoder
// ============================================================================

void test_decode_unpacks_a_record_into_the_json_answer() {
    for (bool compress : codecs()) {
        json got = wire::decode_pop_frames(framed_answer(R"({"frames":true})", 10, 11, compress, two_frames()));
        check(got == json::parse(TWO_MESSAGES_JSON),
              std::string(compress ? "zstd" : "raw") + " record:\n      got:  " + got.dump() +
                  "\n      want: " + TWO_MESSAGES_JSON);
    }
}

void test_decode_keeps_only_the_messages_inside_the_claim() {
    for (bool compress : codecs()) {
        json got = wire::decode_pop_frames(framed_answer(R"({"frames":true})", 11, 11, compress, two_frames()));
        check(got["messages"].size() == 1, "one message inside the claim");
        const json& m = got["messages"][0];
        check(m["offset"] == 11 && m["transactionId"] == "t-11" && m["producerSub"] == "svc", "the second message");
        check(m["id"] == "02000000-0000-7000-8000-000000000000", "its id as text");
    }
}

void test_decode_reads_a_trace_id_an_empty_payload_and_an_answer_without_own_keys() {
    const std::string bytes = framed_answer("", 10, 11, false,
        {stored_frame(1, "t-10", "", "", TRACE), stored_frame(2, "t-11", R"([1,"due"])")});
    json got = wire::decode_pop_frames(bytes);

    check(got["messages"][0]["traceId"] == TRACE, "the trace id as text");
    check(got["messages"][0]["data"].is_null(), "an empty payload is null");
    check(got["messages"][1]["data"] == json::array({1, "due"}), "a payload that is an array");

    // Version 0 has no JSON object in front of the names.
    check(wire::decode_pop_frames(std::string("QPF1\x00", 5) + bytes.substr(9)) == got, "version 0 decodes the same");
}

void test_decode_adds_the_answers_own_keys_without_frames() {
    const std::string own = R"({"frames":true,"conflation":true,"autopilot":{"partitions":4,"batch":50}})";
    json got = wire::decode_pop_frames(framed_answer(own, 10, 11, false, two_frames()));

    check(!got.contains("frames"), "`frames` is not a key of the JSON answer");
    check(got["conflation"] == true, "the conflation echo");
    check(got["autopilot"] == json({{"partitions", 4}, {"batch", 50}}), "the autopilot's choice");
}

void test_decode_rejects_damage() {
    const auto frames = two_frames();
    const std::string bytes = framed_answer(R"({"frames":true})", 10, 11, false, frames);
    const size_t tail = frames[0].size() + frames[1].size();

    check(wire::is_pop_frames(bytes) && !wire::is_pop_frames(TWO_MESSAGES_JSON), "the magic tells the two answers apart");
    check(decode_error(bytes.substr(0, bytes.size() - 3)).find("framed pop answer") != std::string::npos, "a truncated answer");
    check(decode_error(TWO_MESSAGES_JSON).find("not a framed pop answer") != std::string::npos, "a JSON answer");

    std::string future = bytes;
    future[4] = 9;
    check(decode_error(future).find("unknown version 9") != std::string::npos, "an unknown version");

    // A record that declares more frames than it holds.
    std::string short_record = bytes;
    short_record.replace(bytes.size() - tail - 9, 4, le(3, 4));
    check(decode_error(short_record).find("a record is shorter than its 3 frames") != std::string::npos,
          "a record shorter than its count: " + decode_error(short_record));

    std::string bad_codec = bytes;
    bad_codec[bytes.size() - tail - 5] = 7;
    check(decode_error(bad_codec).find("unknown codec 7") != std::string::npos, "an unknown codec");
}

// ============================================================================
// Pop
// ============================================================================

void test_a_pop_sends_no_frames_parameter_by_default() {
    StubServer server(answer(TWO_MESSAGES_JSON));
    QueenClient client({server.url()}, config_with(false, false, false));

    std::vector<Message> messages = client.queue("orders").batch(2).wait(false).pop();

    check(messages.size() == 2, "two messages");
    const auto pops = server.pops();
    check(!pops[0].asked_frames, "the default pop carries no frames parameter");
    // A GET still names application/json, once.
    check_content_type(pops[0], "application/json");
}

void test_a_framed_pop_gives_the_messages_of_the_json_answer() {
    // 2.7.1 hands over typed messages: every field of each is compared, the
    // delivery attempt and the offset included.
    StubServer json_server(answer(TWO_MESSAGES_JSON));
    QueenClient plain({json_server.url()}, config_with(false, false, false));
    const std::vector<Message> json_messages = plain.queue("orders").batch(2).wait(false).pop();
    const json want = fields_of(json_messages);
    check(json_messages.size() == 2 && json_messages[0].queue == "orders" &&
              json_messages[1].transaction_id == "t-11" && json_messages[1].producer_sub == "svc" &&
              json_messages[1].data == json({{"n", 11}, {"s", "x"}}) && json_messages[0].offset == 10LL &&
              json_messages[1].offset == 11LL && json_messages[1].delivery_attempt == 3,
          "the JSON answer's messages: " + want.dump());

    for (bool compress : {false, true}) {
        StubServer server(framed(framed_answer(R"({"frames":true})", 10, 11, compress, two_frames())));
        QueenClient client({server.url()}, config_with(false, false, true));

        const std::vector<Message> messages = client.queue("orders").batch(2).wait(false).pop();
        const json got = fields_of(messages);

        check(server.pops()[0].asked_frames, "the framed pop asks for frames");
        check(got.size() == 2, "two messages");
        check(got == want, std::string(compress ? "zstd" : "raw") + " record:\n      got:  " + got.dump() +
                               "\n      want: " + want.dump());
        check(messages.size() == 2 && messages[0].offset == 10LL && messages[1].offset == 11LL &&
                  messages[0].delivery_attempt == 3 && messages[1].delivery_attempt == 3,
              "the offsets and the delivery attempt of the claim");
    }
}

void test_a_framed_pop_asks_for_frames_on_the_partition_and_namespace_routes_too() {
    StubServer server(nothing());
    QueenClient client({server.url()}, config_with(false, false, true));

    client.queue("orders").partition("p7").wait(false).pop();
    client.queue().namespace_name("ns").wait(false).pop();

    const auto pops = server.pops();
    check(pops.size() == 2, "two pops");
    check(pops[0].target.rfind("/api/v1/pop/queue/orders/partition/p7?", 0) == 0 && pops[0].asked_frames,
          "the partition route: " + pops[0].target);
    check(pops[1].target.rfind("/api/v1/pop?", 0) == 0 && pops[1].asked_frames,
          "the namespace route: " + pops[1].target);
}

void test_a_framed_pop_reads_a_json_answer_as_always() {
    // A broker that does not know the parameter answers JSON.
    StubServer server(answer(TWO_MESSAGES_JSON));
    QueenClient client({server.url()}, config_with(false, false, true));

    const std::vector<Message> got = client.queue("orders").batch(2).wait(false).pop();

    check(got.size() == 2 && got[1].transaction_id == "t-11" && got[1].data == json({{"n", 11}, {"s", "x"}}),
          "the JSON answer's messages: " + fields_of(got).dump());
}

void test_a_framed_pop_of_nothing() {
    StubServer server(nothing());
    QueenClient client({server.url()}, config_with(false, false, true));

    check(client.queue("orders").wait(false).pop().empty(), "an empty pop is no messages");
}

void test_a_framed_pop_carries_the_autopilots_choice() {
    const std::string own = R"({"frames":true,"autopilot":{"partitions":4,"batch":50}})";
    StubServer server(framed(framed_answer(own, 10, 11, false, two_frames())));
    QueenClient client({server.url()}, config_with(false, false, true));

    const PopResult result = client.queue("orders").wait(false).pop_result();

    check(result.messages.size() == 2, "two messages");
    check(result.autopilot.present && result.autopilot.partitions == 4 && result.autopilot.batch == 50,
          "the autopilot's choice from the answer's own keys");
}

void test_a_framed_pop_carries_the_conflation_echo_and_still_throws_without_it() {
    {
        StubServer server(framed(framed_answer(R"({"frames":true,"conflation":true})", 10, 11, false, two_frames())));
        QueenClient client({server.url()}, config_with(false, false, true));
        check(client.queue("orders").group("g").conflation(true).wait(false).pop().size() == 2,
              "a conflating pop whose answer echoes it");
    }
    StubServer server(framed(framed_answer(R"({"frames":true})", 10, 11, false, two_frames())));
    QueenClient client({server.url()}, config_with(false, false, true));
    bool raised = false;
    try {
        client.queue("orders").group("g2").conflation(true).wait(false).pop();
    } catch (const ConflationUnsupportedError&) {
        raised = true;
    }
    check(raised, "no echo, no conflation: the pop raises");
}

void test_consume_with_framed_pops_hands_the_handler_the_claim_in_order() {
    StubServer server(framed(framed_answer(R"({"frames":true})", 10, 11, true, two_frames())));
    QueenClient client({server.url()}, config_with(false, false, true));
    StopAfter budget(5000);
    std::mutex mutex;
    std::vector<std::string> seen;

    // consume() hands over one message per call (2.7.1: .each() is gone).
    client.queue("orders").batch(2).wait(false).auto_ack(false).limit(2).cancel(budget.token())
        .consume([&](const Message& message) {
            std::lock_guard<std::mutex> lock(mutex);
            seen.push_back(message.transaction_id + "@" + message.partition_id + "/" + message.lease_id);
        });

    check(seen == std::vector<std::string>({"t-10@7/lease-1", "t-11@7/lease-1"}),
          "the handler got the claim in order (" + std::to_string(seen.size()) + " messages)");
    const auto pops = server.pops();
    check(!pops.empty(), "the consumer popped");
    for (const auto& pop : pops) check(pop.asked_frames, "a pop of the consumer asks for frames");
}

// ============================================================================
// Transaction
// ============================================================================

/// The message a transaction acks, as a pop handed it over (2.7.1: ack() takes a
/// Message, and its lease and group come from it; this one is in no group).
Message popped() {
    return Message::from_json(json{{"transactionId", "in-1"}, {"partitionId", "7"}, {"leaseId", "lease-1"}});
}

inline Responder committed(const json& results) {
    return answer(json{{"transactionId", "tx"}, {"success", true}, {"results", results}}.dump());
}

struct TransactionBody {
    int flags = 0;
    json head;                      // null for an empty head
    std::vector<FramedGroup> groups;
};

/// The flags, the head and the groups of a binary transaction body.
TransactionBody transaction_body(const std::string& raw) {
    check(raw.substr(0, 4) == "QTB1", "a binary transaction body");
    TransactionBody body;
    body.flags = static_cast<unsigned char>(raw[4]);
    const size_t head_length = Reader(raw.substr(5, 4)).u32();
    const std::string head = raw.substr(9, head_length);
    body.head = head.empty() ? json(nullptr) : json::parse(head);
    body.groups = framed_groups(raw.substr(9 + head_length));
    return body;
}

void test_a_transaction_is_the_json_bundle_by_default() {
    const json results = json::array({{{"index", 0}, {"type", "ack"}, {"success", true}},
                                      {{"index", 1}, {"type", "push"}, {"success", true}}});
    for (const auto& config : {config_with(false, false, false), config_with(true, false, false),
                               config_with(false, true, false)}) {
        StubServer server(committed(results));
        QueenClient client({server.url()}, config);

        client.transaction().ack(popped()).queue("out").push(json{{"n", 1}}).commit();

        const auto sent = server.transactions();
        check(sent.size() == 1, "one transaction request");
        check_content_type(sent[0], "application/json");
        const json body = json::parse(sent[0].body);
        check(body["operations"][0]["type"] == "ack" && body["operations"][1]["type"] == "push",
              "the JSON bundle, its operations as the caller added them");
        check(body["operations"][1]["items"][0]["payload"] == json({{"n", 1}}),
              "the payload travels as it is: " + body["operations"][1].dump());
    }
}

void test_the_pushes_travel_as_frames_and_the_rest_as_the_json_head() {
    StubServer server(committed(json::array({
        {{"index", 0}, {"type", "ack"}, {"success", true}, {"transactionId", "in-1"}},
        {{"index", 1}, {"type", "push"}, {"success", true}, {"transactionId", "a"}},
        {{"index", 2}, {"type", "push"}, {"success", true}, {"transactionId", "tx-b"}},
        {{"index", 3}, {"type", "push"}, {"success", true}, {"transactionId", "c"}}})));
    QueenClient client({server.url()}, config_with(true, true, false));

    // Pushes that name no partition go to the queue's default one.
    TransactionResult result = client.transaction()
        .ack(popped())
        .queue("out").push(json{{"n", 1}})
        .queue("out").push_items({TxPushItem{json{{"n", 2}}, "tx-b", ""}})
        .queue("other").push("text")
        .commit();

    check(result.success && result.results.size() == 4, "the answer: " + result.raw.dump());
    const auto sent = server.transactions();
    check(sent.size() == 1, "one transaction request");
    check_content_type(sent[0], "application/x-queen-push");
    const auto body = transaction_body(sent[0].body);
    check(body.flags == 0, "no flags");
    // The head: the ack and the lease, and no push.
    check(body.head == json({
              {"operations", json::array({{{"type", "ack"}, {"transactionId", "in-1"}, {"partitionId", "7"},
                                           {"status", "completed"}, {"leaseId", "lease-1"}}})},
              {"requiredLeases", json::array({"lease-1"})}}),
          "the head: " + body.head.dump());
    // The pushes: consecutive items of one partition share a group.
    check(body.groups.size() == 2, "two groups");
    if (body.groups.size() != 2) return;
    const auto& first = body.groups[0];
    check(first.queue == "out" && first.partition == "Default" && first.count == 2 && first.frames.size() == 2,
          "the first group: out/Default with two messages");
    check(first.frames[0].payload == R"({"n":1})" && first.frames[0].txn.size() == 36, "its first message");
    check(first.frames[1].payload == R"({"n":2})" && first.frames[1].txn == "tx-b", "its second message");
    const auto& second = body.groups[1];
    check(second.queue == "other" && second.count == 1 && second.frames[0].payload == R"("text")",
          "the second group: other with one message");
}

void test_the_results_come_back_in_the_order_of_the_callers_operations() {
    // The caller pushes, acks, pushes. The broker answers the ack first (it is
    // in the head), then the two pushes, then the rider.
    StubServer server(committed(json::array({
        {{"index", 0}, {"type", "ack"}, {"success", true}, {"transactionId", "in-1"}},
        {{"index", 1}, {"type", "push"}, {"success", true}, {"transactionId", "first"}},
        {{"index", 2}, {"type", "push"}, {"success", true}, {"transactionId", "second"}},
        {{"index", 3}, {"type", "kv"}, {"opIndex", 0}, {"applied", true}}})));
    QueenClient client({server.url()}, config_with(true, true, false));

    TransactionResult result = client.transaction()
        .queue("out").push_items({TxPushItem{1, "first", ""}})
        .ack(popped())
        .queue("out").push_items({TxPushItem{2, "second", ""}})
        .kv("saga").put("k1", json{{"step", 1}}, KvTtl::seconds(86400))
        .commit();

    const json& results = result.results;
    check(results.size() == 4, "four results");
    if (results.size() != 4) return;
    check(results[0]["index"] == 0 && results[0]["type"] == "push" && results[0]["transactionId"] == "first",
          "results[0] is the first push: " + results[0].dump());
    check(results[1]["index"] == 1 && results[1]["type"] == "ack" && results[1]["transactionId"] == "in-1",
          "results[1] is the ack: " + results[1].dump());
    check(results[2]["index"] == 2 && results[2]["type"] == "push" && results[2]["transactionId"] == "second",
          "results[2] is the second push: " + results[2].dump());
    check(results[3]["index"] == 3 && results[3]["type"] == "kv", "the rider stays where it was: " + results[3].dump());
    // On the wire the two pushes are one group, in the caller's order, and the
    // rider travels in the head.
    const auto body = transaction_body(server.transactions()[0].body);
    check(body.head["operations"].size() == 1 && body.head["operations"][0]["type"] == "ack", "the head has the ack only");
    check(body.head["kv"].size() == 1, "the rider travels in the head");
    check(body.groups.size() == 1 && body.groups[0].frames.size() == 2 &&
              body.groups[0].frames[0].txn == "first" && body.groups[0].frames[1].txn == "second",
          "one group, the pushes in the caller's order");
}

void test_the_results_stay_as_they_are_when_the_acks_come_first_anyway() {
    const json results = json::array({
        {{"index", 0}, {"type", "ack"}, {"success", true}, {"transactionId", "in-1"}},
        {{"index", 1}, {"type", "push"}, {"success", true}, {"transactionId", "a"}}});
    StubServer server(committed(results));
    QueenClient client({server.url()}, config_with(true, true, false));

    TransactionResult result = client.transaction().ack(popped())
        .queue("out").push_items({TxPushItem{1, "a", ""}}).commit();

    check(result.results == results, "the results as the broker sent them: " + result.results.dump());
}

void test_a_rolled_back_bundle_comes_back_as_the_broker_answered_it() {
    // A lost precondition is the verdict, RETURNED (2.7.1: typed, with the raw answer).
    const json lost = {{"transactionId", "tx"}, {"success", false}, {"reason", "kv_precondition"},
                       {"results", json::array()}, {"failedIndex", 2}, {"kvReason", "exists"}};
    StubServer server(answer(lost.dump()));
    QueenClient client({server.url()}, config_with(true, true, false));
    KvWriteOptions required;
    required.required = true;

    TransactionResult result = client.transaction()
        .queue("out").push(1)
        .ack(popped())
        .kv("idem").put_if_absent("order-1", true, KvTtl::seconds(86400), required)
        .commit();

    check(result.raw == lost, "the verdict as the broker sent it: " + result.raw.dump());
    check(!result.success && result.lost_precondition() && result.failed_index == size_t{2} &&
              result.kv_reason == "exists",
          "the verdict, read: reason '" + result.reason + "', kvReason '" + result.kv_reason + "'");
    check(server.transactions()[0].body.substr(0, 4) == "QTB1", "the bundle went out as the binary body");

    // Any other rollback is THROWN since 2.7.1 (TransactionError), still carrying
    // the broker's answer.
    const json rejected = {{"transactionId", "tx"}, {"success", false}, {"reason", "rejected_ack"},
                           {"error", "QTXN lease expired; the transaction rolled back"},
                           {"results", json::array()}};
    StubServer refusing(answer(rejected.dump()));
    QueenClient other({refusing.url()}, config_with(true, true, false));
    bool thrown = false;
    try {
        other.transaction().queue("out").push(1).ack(popped()).commit();
    } catch (const TransactionError& e) {
        thrown = true;
        check(e.result().raw == rejected && e.result().reason == "rejected_ack",
              "the rollback as the broker sent it: " + e.result().raw.dump());
    }
    check(thrown, "a rollback other than a lost precondition throws TransactionError");
    check(refusing.transactions()[0].body.substr(0, 4) == "QTB1", "that bundle went out as the binary body too");
}

void test_a_bundle_whose_push_carries_a_trace_id_stays_json() {
    StubServer server(committed(json::array({{{"index", 0}, {"type", "push"}, {"success", true}}})));
    QueenClient client({server.url()}, config_with(true, true, false));

    // A transaction's push is the one that can carry a trace id.
    client.transaction().queue("out").push_items({TxPushItem{1, "", TRACE}}).commit();

    const auto sent = server.transactions();
    check_content_type(sent[0], "application/json");
    check(json::parse(sent[0].body)["operations"][0]["items"][0]["traceId"] == TRACE, "with its trace id");
}

void test_a_bundle_without_pushes_stays_json() {
    // There is nothing to frame.
    StubServer server(committed(json::array({{{"index", 0}, {"type", "ack"}, {"success", true}}})));
    QueenClient client({server.url()}, config_with(true, true, false));

    client.transaction().ack(popped()).commit();

    const auto sent = server.transactions();
    check_content_type(sent[0], "application/json");
    check(sent[0].body[0] == '{', "a bundle of acks travels as JSON");
}

void test_the_transaction_counts_flag_and_an_empty_head() {
    const std::string body = wire::encode_transaction_frames("", {{"q", "p", {"1"}, std::nullopt}}, true);

    check(body.substr(0, 9) == std::string("QTB1\x02\x00\x00\x00\x00", 9), "flag 2 and an empty head");
    const auto decoded = transaction_body(body);
    check(decoded.flags == 2 && decoded.head.is_null(), "the counts flag, no head");
    check(decoded.groups.size() == 1 && decoded.groups[0].frames[0].payload == "1", "the group");
}

void test_transaction_frames_needs_a_push_it_can_carry() {
    const json ack = {{"type", "ack"}, {"transactionId", "in-1"}, {"partitionId", "7"}, {"status", "completed"}};
    check(!wire::transaction_frames({{"operations", json::array({ack})}}), "a bundle of acks");
    check(!wire::transaction_frames({{"operations", json::array()}}), "no operations");
    check(!wire::transaction_frames({{"kv", json::array()}}), "no operations key");
    check(!wire::transaction_frames({{"operations", json::array({{{"type", "push"}, {"items", json::array({
              {{"queue", "q"}, {"payload", 1}, {"traceId", TRACE}}})}}})}}),
          "a push with a trace id");
    // The order the broker answers in, when it is not the caller's.
    json answer = {{"results", json::array({{{"index", 0}, {"type", "ack"}}, {{"index", 1}, {"type", "push"}}})}};
    wire::restore_result_order(answer, std::vector<size_t>{1, 0});
    check(answer["results"][0] == json({{"index", 0}, {"type", "push"}}) &&
              answer["results"][1] == json({{"index", 1}, {"type", "ack"}}),
          "the results in the caller's order: " + answer["results"].dump());
}

// ============================================================================
// Transport
// ============================================================================

/// 2.7.1 keeps its connections alive in a pool (2.7.0 opened one per request): a
/// run of sequential requests -- pushes, a pop, a transaction, a delete --
/// arrives on ONE connection, which then waits in the pool. And each names its
/// Content-Type exactly once: a GET and a DELETE name application/json too.
void test_sequential_requests_share_one_pooled_connection() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        res.status = 200;
        if (req.path == "/api/v1/push") {
            res.set_content(R"([{"status":"queued"}])", "application/json");
        } else if (req.path == "/api/v1/transaction") {
            res.set_content(R"({"transactionId":"tx","success":true,"results":[{"index":0,"type":"push","success":true}]})",
                            "application/json");
        } else if (req.path.rfind("/api/v1/pop", 0) == 0) {
            res.set_content(TWO_MESSAGES_JSON, "application/json");
        } else {
            res.set_content(R"({"deleted":true})", "application/json");
        }
    });
    QueenClient client({server.url()}, config_with(false, false, false));

    client.queue("orders").partition("p1").push(json{{"a", 1}});
    check(client.queue("orders").batch(2).wait(false).pop().size() == 2, "the pop's two messages");
    client.transaction().queue("out").push(json{{"n", 1}}).commit();
    client.queue("orders").partition("p1").push(json{{"a", 2}});
    client.queue("orders").del();

    const auto calls = server.calls();
    check(calls.size() == 5, "five requests, got " + std::to_string(calls.size()));
    std::set<int> connections;
    for (const auto& call : calls) {
        connections.insert(call.remote_port);
        check_content_type(call, "application/json");
    }
    check(connections.size() == 1, std::to_string(calls.size()) + " sequential requests came on " +
                                       std::to_string(connections.size()) + " connections, not one");
    check(calls.size() == 5 && calls[0].method == "POST" && calls[1].method == "GET" &&
              calls[2].method == "POST" && calls[3].method == "POST" && calls[4].method == "DELETE",
          "a POST, a GET, a POST, a POST and a DELETE");
    const size_t idle = client.get_http_client()->idle_connections();
    check(idle == 1, "the connection waits in the pool afterwards: " + std::to_string(idle) + " idle");
}

// ============================================================================
// main
// ============================================================================

int main() {
    std::cout << BLUE << "Queen C++ client - binary push and framed pop wire tests"
              << (wire::zstd_available() ? " (with zstd)" : " (without zstd)") << RESET
              << std::endl
              << std::endl;

    run_test("a push is JSON by default", test_a_push_is_json_by_default);
    run_test("binary_push sends the binary body, each item with its transaction id",
             test_binary_push_sends_the_binary_body_with_each_transaction_id);
    run_test("consecutive items of one partition share a group",
             test_consecutive_items_of_one_partition_share_a_group);
    run_test("binary_push carries payloads that are not objects, and non-ASCII text",
             test_binary_push_carries_payloads_that_are_not_objects_and_non_ascii_text);
    run_test("a plain push carries no trace id", test_a_plain_push_carries_no_trace_id);
    run_test("an item the binary body does not carry leaves the push to JSON",
             test_an_item_the_binary_body_does_not_carry_leaves_the_push_to_json);
    run_test("a buffered push is binary too", test_a_buffered_push_is_binary_too);
    run_test("the counts flag, and mismatched transaction ids",
             test_the_counts_flag_and_mismatched_transaction_ids);
    run_zstd_test("the zstd flag compresses the whole body", test_the_zstd_flag_compresses_the_whole_body);
    run_zstd_test("push_compression sends each partition as one zstd frame of stored frames",
                  test_push_compression_sends_each_partition_as_one_zstd_frame_of_stored_frames);
    run_zstd_test("a frame without a transaction id gets the text of its id",
                  test_a_frame_without_a_transaction_id_gets_the_text_of_its_id);
    run_test("the zstd options say what they need", test_the_zstd_options_say_what_they_need);

    run_test("decode unpacks a record into the JSON answer", test_decode_unpacks_a_record_into_the_json_answer);
    run_test("decode keeps only the messages inside the claim",
             test_decode_keeps_only_the_messages_inside_the_claim);
    run_test("decode reads a trace id, an empty payload, and an answer without own keys",
             test_decode_reads_a_trace_id_an_empty_payload_and_an_answer_without_own_keys);
    run_test("decode adds the answer's own keys, without `frames`",
             test_decode_adds_the_answers_own_keys_without_frames);
    run_test("decode rejects damage", test_decode_rejects_damage);

    run_test("a pop sends no frames parameter by default", test_a_pop_sends_no_frames_parameter_by_default);
    run_zstd_test("a framed pop gives the messages of the JSON answer",
                  test_a_framed_pop_gives_the_messages_of_the_json_answer);
    run_zstd_test("a framed pop asks for frames on the partition and namespace routes too",
                  test_a_framed_pop_asks_for_frames_on_the_partition_and_namespace_routes_too);
    run_zstd_test("a framed pop reads a JSON answer as always", test_a_framed_pop_reads_a_json_answer_as_always);
    run_zstd_test("a framed pop of nothing", test_a_framed_pop_of_nothing);
    run_zstd_test("a framed pop carries the autopilot's choice", test_a_framed_pop_carries_the_autopilots_choice);
    run_zstd_test("a framed pop carries the conflation echo, and still throws without it",
                  test_a_framed_pop_carries_the_conflation_echo_and_still_throws_without_it);
    run_zstd_test("consume with framed pops hands the handler the claim in order",
                  test_consume_with_framed_pops_hands_the_handler_the_claim_in_order);

    run_test("a transaction is the JSON bundle by default", test_a_transaction_is_the_json_bundle_by_default);
    run_zstd_test("a transaction's pushes travel as frames, the rest as the JSON head",
                  test_the_pushes_travel_as_frames_and_the_rest_as_the_json_head);
    run_zstd_test("a transaction's results come back in the order of the caller's operations",
                  test_the_results_come_back_in_the_order_of_the_callers_operations);
    run_zstd_test("a transaction's results stay as they are when the acks come first anyway",
                  test_the_results_stay_as_they_are_when_the_acks_come_first_anyway);
    run_zstd_test("a rolled-back bundle comes back as the broker answered it",
                  test_a_rolled_back_bundle_comes_back_as_the_broker_answered_it);
    run_zstd_test("a bundle whose push carries a trace id stays JSON",
                  test_a_bundle_whose_push_carries_a_trace_id_stays_json);
    run_zstd_test("a bundle without pushes stays JSON", test_a_bundle_without_pushes_stays_json);
    run_zstd_test("the transaction counts flag, and an empty head",
                  test_the_transaction_counts_flag_and_an_empty_head);
    run_test("transaction_frames needs a push it can carry", test_transaction_frames_needs_a_push_it_can_carry);

    run_test("sequential requests share one pooled connection, each naming its Content-Type once",
             test_sequential_requests_share_one_pooled_connection);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All binary push and framed pop wire tests passed" << RESET;
        if (skipped > 0) std::cout << YELLOW << " (" << skipped << " skipped: built without zstd)" << RESET;
        std::cout << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
