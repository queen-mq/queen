/**
 * Queen C++ Client - routing without copies, wire tests
 *
 * WHAT THIS FILE IS FOR.
 * Four features stand on one broker engine (PLAN_ROUTING.md, catalogue
 * version 8): message headers, filtered consumer groups, reading queues and
 * `link` in transactions. A live broker serves any of these requests without
 * saying which bytes it got, so what is pinned here is the wire:
 *
 *   1. HEADERS travel on every push form -- the JSON push, a buffered push, a
 *      transaction push, a timer, and the frames of push_compression and of
 *      the binary transaction (flag 8 and its block, byte for byte) -- and come
 *      back on pops, framed pops and fetches. A push without headers is the
 *      request it always was, and the plain binary body, which has no room for
 *      them, leaves such a push to JSON.
 *   2. A FILTERED GROUP sends the same `where` on every pop, as the JSON list
 *      or as one string, and a misuse (no group, a discovery pop, an unknown
 *      op, both forms at once) is refused before anything is sent. An answer
 *      without the broker's `"filtered":true` -- a 204 included -- is a broker
 *      that would deliver everything: refused on the first round trip.
 *   3. A READING QUEUE's configure carries from, where, partitionBy and start,
 *      and none of the settings its sources decide unless the caller set one;
 *      an answer without a `route` is a broker that does not read queues.
 *   4. LINK is one operation per message, naming the message as its pop
 *      returned it, and rides the binary transaction in its JSON head.
 *
 * Like its siblings this runs against an in-process httplib::Server -- no
 * broker:
 *
 *   make routing && ./bin/test_routing
 *
 * The framed cases need zstd (the Makefile adds -DQUEEN_CLIENT_ZSTD when the
 * library is installed); without it they are skipped.
 *
 * Siblings: test_wire_forms.cpp, test_reads.cpp, test_conflation.cpp.
 */

#include "queen_client.hpp"
#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace queen;
using json = nlohmann::json;

#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define YELLOW "\033[33m"
#define RESET "\033[0m"

// ============================================================================
// Test double
// ============================================================================

struct RecordedCall {
    std::string method;
    std::string path;          // without the query
    std::string target;        // raw request target, query string included
    httplib::Params params;    // the query, decoded
    std::string content_type;
    std::string body;          // as bytes: a binary body is not text

    bool has_param(const std::string& key) const { return params.count(key) > 0; }

    std::string param(const std::string& key) const {
        auto it = params.find(key);
        return it == params.end() ? std::string() : it->second;
    }
};

using Responder = std::function<void(const httplib::Request&, httplib::Response&)>;

inline Responder answer(const std::string& body, int status = 200,
                        const std::string& content_type = "application/json") {
    return [body, status, content_type](const httplib::Request&, httplib::Response& res) {
        res.status = status;
        res.set_content(body, content_type);
    };
}

inline Responder queued(size_t count = 1) {
    json items = json::array();
    for (size_t i = 0; i < count; ++i) items.push_back({{"status", "queued"}});
    return answer(items.dump());
}

inline Responder nothing() {
    return [](const httplib::Request&, httplib::Response& res) { res.status = 204; };
}

/// An empty pop of a filtered group, as a broker that filters answers it: a
/// 200 that says so, never a bodiless 204.
inline Responder filtered_empty() {
    return answer(R"({"success":true,"messages":[],"filtered":true})");
}

inline Responder committed(const json& results) {
    return answer(json{{"transactionId", "tx"}, {"success", true}, {"results", results}}.dump());
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
            if (call.method == method && call.path.rfind(prefix, 0) == 0) out.push_back(call);
        }
        return out;
    }

public:
    explicit StubServer(Responder responder) : responder_(std::move(responder)) {
        auto handler = [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                calls_.push_back({req.method, req.path, req.target, req.params,
                                  req.get_header_value("Content-Type"), req.body});
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
    std::vector<RecordedCall> configures() const { return matching("POST", "/api/v1/configure"); }
};

/// Cancels a consume loop after a budget, so a loop that fails to stop on its
/// own FAILS its test instead of hanging it.
class StopAfter {
private:
    Cancel token_;
    std::atomic<bool> done_{false};
    std::thread timer_;

public:
    explicit StopAfter(int budget_millis) {
        timer_ = std::thread([this, budget_millis]() {
            auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budget_millis);
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

/// A test of the framed forms: run where the build has zstd.
void run_zstd_test(const std::string& name, std::function<void()> test_fn) {
    if (wire::zstd_available()) {
        run_test(name, std::move(test_fn));
        return;
    }
    ++skipped;
    std::cout << YELLOW << "SKIP: " << name << " (built without zstd)" << RESET << std::endl;
}

/// What `fn` throws as std::invalid_argument, or "" when it throws nothing.
/// Any other exception escapes and fails the case.
std::string refusal(const std::function<void()>& fn) {
    try {
        fn();
        return "";
    } catch (const std::invalid_argument& e) {
        return e.what();
    }
}

/// What `fn` throws as RoutingUnsupportedError (a broker without routing), or
/// "" when it throws nothing. Any other exception escapes and fails the case.
std::string unsupported(const std::function<void()>& fn) {
    try {
        fn();
        return "";
    } catch (const RoutingUnsupportedError& e) {
        return e.what();
    }
}

ClientConfig config_with(bool binary_push = false, bool push_compression = false, bool framed_pop = false) {
    ClientConfig config;
    config.retry_attempts = 1;
    config.binary_push = binary_push;
    config.push_compression = push_compression;
    config.framed_pop = framed_pop;
    return config;
}

// ============================================================================
// Bytes
// ============================================================================

std::string le(uint64_t value, size_t width) {
    std::string out;
    for (size_t i = 0; i < width; ++i) out.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
    return out;
}

std::string text(const std::string& s) { return le(s.size(), 2) + s; }

/// A headers block written out by hand: u16 count | (u8 len, name | u16 len, value) x count.
std::string block_of(const std::vector<std::pair<std::string, std::string>>& pairs) {
    std::string out = le(pairs.size(), 2);
    for (const auto& pair : pairs) out += le(pair.first.size(), 1) + pair.first + text(pair.second);
    return out;
}

/// Reads the fields of a binary body in order.
struct Reader {
    std::string data;
    size_t at = 0;

    explicit Reader(std::string bytes) : data(std::move(bytes)) {}

    std::string take(size_t n) {
        if (at + n > data.size()) throw std::runtime_error("a binary body ends early");
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

struct ClientFrame {
    int flags = 0;
    std::string txn;
    std::string headers;   // the block, "" without flag 8
    std::string payload;
};

struct FramedGroup {
    std::string queue;
    std::string partition;
    size_t count = 0;
    std::vector<ClientFrame> frames;
};

/// The groups of a framed body, each frame read as the broker reads a
/// client's: u32 body_len | flags | id | u16 txn | [u16 headers, flag 8] | payload.
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
            ClientFrame frame;
            frame.flags = static_cast<unsigned char>(body.take(1)[0]);
            body.take(16);  // the message id
            frame.txn = body.text16();
            if (frame.flags & 8) frame.headers = body.text16();
            frame.payload = body.take(body.left());
            group.frames.push_back(std::move(frame));
        }
        groups.push_back(std::move(group));
    }
    check(r.left() == 0, "bytes left after the last group");
    return groups;
}

struct TransactionBody {
    json head;
    std::vector<FramedGroup> groups;
};

/// The head and the groups of a binary transaction body.
TransactionBody transaction_body(const std::string& raw) {
    check(raw.substr(0, 4) == "QTB1", "a binary transaction body");
    TransactionBody body;
    const size_t head_length = Reader(raw.substr(5, 4)).u32();
    const std::string head = raw.substr(9, head_length);
    body.head = head.empty() ? json(nullptr) : json::parse(head);
    body.groups = framed_groups(raw.substr(9 + head_length));
    return body;
}

/// One stored frame, as a framed pop answer carries it:
/// u32 body_len | flags | id | txn | [producer, flag 2] | [headers, flag 8] | payload.
std::string stored_frame(int id, int flags, const std::string& txn, const std::string& payload,
                         const std::string& producer = "", const std::string& headers = "") {
    std::string id_bytes(16, '\0');
    id_bytes[0] = static_cast<char>(id);
    id_bytes[6] = 0x70;
    id_bytes[8] = static_cast<char>(0x80);
    std::string body(1, static_cast<char>(flags));
    body += id_bytes;
    body += text(txn);
    if (flags & 2) body += text(producer);
    if (flags & 8) body += text(headers);
    body += payload;
    return le(body.size(), 4) + body;
}

/// A framed pop answer of one claim on partition 7 ("p7") of "orders": one
/// raw record at base offset 10 holding `frames`, all inside the claim, with
/// `own` (the answer's own keys, the echoes among them) in front.
std::string framed_answer(const std::vector<std::string>& frames, const std::string& own = "") {
    std::string raw;
    for (const auto& frame : frames) raw += frame;
    std::string b("QPF1\x01", 5);
    b += le(own.size(), 4) + own;
    b += text("orders") + text("lease-1") + text("billing");
    b += le(1, 4);                                    // claims
    b += le(7, 8) + text("p7") + le(10, 8) + le(10 + frames.size() - 1, 8);
    b += le(1, 4);                                    // delivery attempt
    b += le(1, 4);                                    // records
    b += le(10, 8) + le(1700000000000000ULL, 8) + le(frames.size(), 4);
    b.push_back('\0');                                // raw codec
    b += le(raw.size(), 4) + raw;
    return b;
}

// ============================================================================
// Fixtures
// ============================================================================

// A pop answer of partition 7 ("order-1") of "orders" for group "billing": the
// first message has headers, the second none.
static const std::string POP_WITH_HEADERS =
    R"({"success":true,"queue":"orders","partition":"order-1","partitionId":"7","leaseId":"lease-1","consumerGroup":"billing","messages":[)"
    R"({"id":"01000000-0000-7000-8000-000000000000","transactionId":"t1","traceId":null,"data":{"total":120},"producerSub":null,"headers":{"traceparent":"00-abc-01","type":"order.paid"},"createdAt":"2026-11-12T10:00:00.000Z","partitionId":"7","partition":"order-1","leaseId":"lease-1","consumerGroup":"billing","deliveryAttempt":1,"offset":42},)"
    R"({"id":"02000000-0000-7000-8000-000000000000","transactionId":"t2","traceId":null,"data":{"total":80},"producerSub":null,"createdAt":"2026-11-12T10:00:01.000Z","partitionId":"7","partition":"order-1","leaseId":"lease-1","consumerGroup":"billing","deliveryAttempt":1,"offset":43})"
    R"(],"partitionsClaimed":1})";

/// The two messages of POP_WITH_HEADERS, as a pop hands them over.
std::vector<Message> popped() {
    return util::messages_of(json::parse(POP_WITH_HEADERS));
}

// ============================================================================
// Headers: push
// ============================================================================

void test_a_push_carries_headers_only_when_it_has_some() {
    StubServer server(queued(2));
    QueenClient client({server.url()}, config_with());

    client.queue("orders").partition("order-1").push_items({
        PushItem{json{{"total", 120}}, "t1", {{"type", "order.paid"}, {"traceparent", "00-abc-01"}}},
        PushItem{json{{"total", 80}}, "t2"}});
    client.queue("orders").partition("order-1").push(json{{"total", 5}}, "t3", {{"type", "order.refunded"}});

    const auto sent = server.pushes();
    check(sent.size() == 2, "two push requests");
    if (sent.size() != 2) return;
    check(sent[0].content_type == "application/json", "a JSON push");
    const json items = json::parse(sent[0].body)["items"];
    check(items[0]["headers"] == json({{"traceparent", "00-abc-01"}, {"type", "order.paid"}}),
          "the first item carries its headers: " + items[0].dump());
    // An item without headers is the item every broker knows: queue,
    // partition, payload and transaction id, and nothing else.
    check(items[1].size() == 4 && !items[1].contains("headers"), "the second has none: " + items[1].dump());
    const json third = json::parse(sent[1].body)["items"][0];
    check(third["transactionId"] == "t3" && third["headers"] == json({{"type", "order.refunded"}}),
          "push(payload, id, headers): " + third.dump());
}

void test_a_buffered_push_keeps_each_items_headers() {
    StubServer server(queued(2));
    QueenClient client({server.url()}, config_with());
    // A batch the pushes never fill, so the flush below is what sends them.
    BufferOptions batches;
    batches.message_count = 10;
    batches.time_millis = 60000;

    const auto result = client.queue("orders").partition("order-1").buffer(batches).push_items({
        PushItem{json{{"n", 1}}, "t1", {{"type", "a"}}},
        PushItem{json{{"n", 2}}, "t2"}});
    check(result.empty(), "a buffered push has no verdict yet");
    client.flush_all_buffers();

    json items = json::array();
    for (const auto& call : server.pushes()) {
        const json body = json::parse(call.body);
        for (const auto& item : body["items"]) items.push_back(item);
    }
    check(items.size() == 2, "both messages flushed: " + items.dump());
    if (items.size() != 2) return;
    check(items[0]["transactionId"] == "t1" && items[0]["headers"] == json({{"type", "a"}}),
          "the first keeps its headers: " + items[0].dump());
    check(!items[1].contains("headers"), "the second has none: " + items[1].dump());
    client.close();
}

void test_a_header_name_the_broker_refuses_is_refused_before_anything_is_sent() {
    StubServer server(queued());
    QueenClient client({server.url()}, config_with());
    auto push_with = [&](const Headers& headers) {
        return refusal([&] { client.queue("orders").push(json{{"n", 1}}, "t1", headers); });
    };

    check(push_with({{"Type", "x"}}).find("a-z") != std::string::npos, "an upper-case name");
    check(!push_with({{"", "x"}}).empty(), "an empty name");
    check(!push_with({{"a b", "x"}}).empty(), "a name with a space");
    check(!push_with({{std::string(65, 'a'), "x"}}).empty(), "a name of 65 bytes");
    check(push_with({{"queen-trace", "x"}}).find("queen-") != std::string::npos, "a name the broker keeps");
    check(server.calls().empty(), "nothing sent for any of them");

    // The edges of what is allowed.
    check(push_with({{std::string(64, 'z'), ""}, {"a.b_c-9", "\xc3\xa9t\xc3\xa9"}}).empty(),
          "a name of 64 bytes, every character class, an empty value and a non-ASCII one");
    check(server.pushes().size() == 1, "and that one is sent");

    // The same rule on the transaction's pushes and links.
    check(!refusal([&] {
              client.transaction().queue("out").push_items({TxPushItem{1, "t", "", {{"Bad", "x"}}}});
          }).empty(),
          "a transaction push");
    check(!refusal([&] {
              client.transaction().queue("review").link(popped()[0], {{"queen-x", "y"}});
          }).empty(),
          "a link");
    check(server.transactions().empty(), "no transaction sent");
}

void test_encode_headers_writes_the_block_in_name_order() {
    const std::string block = wire::encode_headers({{"type", "order.paid"}, {"a", ""}, {"b-2", "x y"}});
    check(block == block_of({{"a", ""}, {"b-2", "x y"}, {"type", "order.paid"}}),
          "the block, sorted by name");
    check(wire::decode_headers(block) == Headers({{"type", "order.paid"}, {"a", ""}, {"b-2", "x y"}}),
          "and it reads back");
    check(wire::encode_headers({}).empty(), "no headers is no block, not a count of 0");

    bool too_long = false;
    try {
        wire::encode_headers({{std::string(256, 'n'), "v"}});
    } catch (const wire::PushBinaryTooLong&) {
        too_long = true;
    }
    check(too_long, "a name its u8 cannot hold");
    too_long = false;
    try {
        wire::encode_headers({{"big", std::string(70000, 'v')}});
    } catch (const wire::PushBinaryTooLong&) {
        too_long = true;
    }
    check(too_long, "a value its u16 cannot hold");

    bool malformed = false;
    try {
        wire::decode_headers(block + "x");
    } catch (const Error&) {
        malformed = true;
    }
    check(malformed, "a block with bytes after its headers does not decode");
}

void test_the_plain_binary_push_leaves_a_push_with_headers_to_json() {
    StubServer server(queued());
    QueenClient client({server.url()}, config_with(true, false, false));

    client.queue("orders").partition("p1").push(json{{"n", 1}}, "t1", {{"type", "a"}});
    client.queue("orders").partition("p1").push(json{{"n", 2}}, "t2");

    const auto sent = server.pushes();
    check(sent.size() == 2, "two pushes");
    if (sent.size() != 2) return;
    // The plain body has no room for headers: the JSON push carries them.
    check(sent[0].content_type == "application/json", "with headers: JSON, got " + sent[0].content_type);
    check(json::parse(sent[0].body)["items"][0]["headers"] == json({{"type", "a"}}), "with its headers");
    check(sent[1].content_type == "application/x-queen-push" && sent[1].body.substr(0, 4) == "QPB1",
          "without headers: the binary body, as before");

    const json item = {{"queue", "q"}, {"payload", 1}, {"headers", {{"k", "v"}}}};
    check(!wire::binary_push_body(json::array({item}), false), "binary_push_body: none without frames");
    const json none = {{"queue", "q"}, {"payload", 1}, {"headers", json::object()}};
    check(wire::binary_push_body(json::array({none}), false).has_value(), "empty headers are none");
    bool refused = false;
    try {
        wire::encode_push_binary({{"q", "p", {"1"}, std::nullopt, std::vector<std::string>{block_of({{"k", "v"}})}}});
    } catch (const std::invalid_argument&) {
        refused = true;
    }
    check(refused, "encode_push_binary refuses a batch that carries headers");
}

void test_a_framed_push_carries_headers_in_its_frames() {
    StubServer server(queued(3));
    QueenClient client({server.url()}, config_with(true, true, false));

    client.queue("orders").partition("p1").push_items({
        PushItem{json{{"n", 1}}, "t1", {{"type", "order.paid"}, {"source", "web"}}},
        PushItem{json{{"n", 2}}, "t2"},
        PushItem{"text", "t3", {{"k", ""}}}});

    const auto sent = server.pushes();
    check(sent.size() == 1, "one push");
    if (sent.size() != 1) return;
    check(sent[0].content_type == "application/x-queen-push" &&
              sent[0].body.substr(0, 5) == std::string("QPB1\x08", 5),
          "the framed body");
    const auto groups = framed_groups(sent[0].body.substr(5));
    check(groups.size() == 1 && groups[0].frames.size() == 3, "one group of three frames");
    if (groups.size() != 1 || groups[0].frames.size() != 3) return;
    const auto& frames = groups[0].frames;
    check(frames[0].flags == 8 && frames[0].txn == "t1" &&
              frames[0].headers == block_of({{"source", "web"}, {"type", "order.paid"}}) &&
              frames[0].payload == R"({"n":1})",
          "the first frame: flag 8, its block after the transaction id, then the payload");
    check(frames[1].flags == 0 && frames[1].headers.empty() && frames[1].payload == R"({"n":2})",
          "a message without headers: flag 0, no block");
    check(frames[2].flags == 8 && frames[2].headers == block_of({{"k", ""}}) && frames[2].payload == R"("text")",
          "an empty value is still a header");

    // A value its frame cannot hold travels as JSON, where the broker says why.
    client.queue("orders").partition("p1").push(json{{"n", 4}}, "t4", {{"big", std::string(70000, 'v')}});
    const auto again = server.pushes();
    check(again.size() == 2 && again[1].content_type == "application/json",
          "a header value over 65535 bytes: the JSON push");
}

// ============================================================================
// Headers: transactions
// ============================================================================

void test_a_transaction_push_carries_headers() {
    StubServer server(committed(json::array({{{"index", 0}}, {{"index", 1}}, {{"index", 2}}})));
    QueenClient client({server.url()}, config_with());

    client.transaction()
        .queue("out").partition("p").push(json{{"n", 1}}, "tx-1", {{"type", "a"}})
        .queue("out").push_items({TxPushItem{2, "tx-2", "", {{"type", "b"}}}, TxPushItem{3, "tx-3", ""}})
        .commit();

    const auto sent = server.transactions();
    check(sent.size() == 1 && sent[0].content_type == "application/json", "one JSON transaction");
    if (sent.size() != 1) return;
    const json ops = json::parse(sent[0].body)["operations"];
    check(ops[0]["items"][0]["headers"] == json({{"type", "a"}}), "push(payload, id, headers): " + ops[0].dump());
    check(ops[1]["items"][0]["headers"] == json({{"type", "b"}}), "a TxPushItem's headers: " + ops[1].dump());
    check(!ops[1]["items"][1].contains("headers"), "an item without headers carries none");
}

void test_a_binary_transaction_carries_headers_in_its_frames_and_links_in_its_head() {
    // The caller acks, pushes, links. The broker answers the head's operations
    // first (the ack, the link), then the push.
    StubServer server(committed(json::array({
        {{"index", 0}, {"type", "ack"}, {"success", true}},
        {{"index", 1}, {"type", "link"}, {"status", "queued"}},
        {{"index", 2}, {"type", "push"}, {"status", "queued"}}})));
    QueenClient client({server.url()}, config_with(true, true, false));
    const auto messages = popped();

    TransactionResult result = client.transaction()
        .ack(messages[0])
        .queue("out").partition("p").push(json{{"n", 1}}, "tx-1", {{"type", "a"}})
        .queue("review").partition("order-1").link(messages[0], {{"reason", "amount"}})
        .commit();

    const auto sent = server.transactions();
    check(sent.size() == 1 && sent[0].content_type == "application/x-queen-push", "the binary transaction");
    if (sent.size() != 1) return;
    const auto body = transaction_body(sent[0].body);
    const json& head_ops = body.head["operations"];
    check(head_ops.size() == 2 && head_ops[0]["type"] == "ack" && head_ops[1]["type"] == "link",
          "the head: the ack and the link: " + body.head.dump());
    check(head_ops[1]["headers"] == json({{"reason", "amount"}}), "the link keeps its headers in the head");
    check(body.groups.size() == 1 && body.groups[0].frames.size() == 1, "the push as one frame");
    if (body.groups.size() == 1 && body.groups[0].frames.size() == 1) {
        const auto& frame = body.groups[0].frames[0];
        check(frame.flags == 8 && frame.txn == "tx-1" && frame.headers == block_of({{"type", "a"}}) &&
                  frame.payload == R"({"n":1})",
              "the pushed frame carries its headers");
    }
    // The results in the order of the caller's own operations.
    const json& results = result.results;
    check(results.size() == 3 && results[0]["type"] == "ack" && results[1]["type"] == "push" &&
              results[2]["type"] == "link" && results[1]["index"] == 1 && results[2]["index"] == 2,
          "results in the caller's order: " + results.dump());
}

// ============================================================================
// Headers: timers
// ============================================================================

void test_a_timer_carries_headers_standalone_and_as_a_rider() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        res.status = 200;
        if (req.path == "/api/v1/transaction") {
            res.set_content(R"({"transactionId":"tx","success":true,"results":[{"index":0},{"index":1}]})",
                            "application/json");
        } else if (req.method == "GET") {
            res.set_content(R"({"found":true,"queue":"reminders","timerKey":"order-1","payload":"e30=",)"
                            R"("headers":{"type":"reminder"}})",
                            "application/json");
        } else {
            res.set_content(R"({"results":[{"ok":true,"status":"scheduled"}]})", "application/json");
        }
    });
    QueenClient client({server.url()}, config_with());

    TimerSchedule timer;
    timer.queue = "reminders";
    timer.timer_key = "order-1";
    timer.payload = json{{"orderId", 1}};
    timer.delay_millis = 60000;
    timer.headers = {{"type", "reminder"}, {"traceparent", "00-abc-01"}};
    client.timers().schedule(timer);
    client.transaction().ack(popped()[0]).timers().schedule(timer).commit();
    TimerSchedule bare = timer;
    bare.headers.clear();
    client.timers().schedule(bare);

    std::vector<RecordedCall> schedules;
    for (const auto& call : server.calls()) {
        if (call.method == "POST" && call.path == "/api/v1/timers") schedules.push_back(call);
    }
    const json want = {{"traceparent", "00-abc-01"}, {"type", "reminder"}};
    check(schedules.size() == 2, "two standalone schedules");
    if (schedules.size() == 2) {
        check(json::parse(schedules[0].body)["operations"][0]["headers"] == want,
              "the schedule carries them: " + schedules[0].body);
        check(!json::parse(schedules[1].body)["operations"][0].contains("headers"),
              "a timer without headers sends none: " + schedules[1].body);
    }
    const auto txns = server.transactions();
    check(txns.size() == 1 && json::parse(txns[0].body)["timers"][0]["headers"] == want,
          "the transaction's timers rider carries them");

    // A name the broker refuses is refused before anything is sent.
    TimerSchedule bad = timer;
    bad.headers = {{"Type", "x"}};
    const size_t before = server.calls().size();
    check(!refusal([&] { client.timers().schedule(bad); }).empty(), "a bad name on a timer");
    check(!refusal([&] { client.transaction().timers().schedule(bad); }).empty(), "a bad name on a rider");
    check(server.calls().size() == before, "nothing sent");

    // peek shows the headers the fired message will carry, as the broker keeps them.
    check(client.timers().peek("reminders", "order-1")["headers"] == json({{"type", "reminder"}}),
          "peek shows them");
}

// ============================================================================
// Headers: reads
// ============================================================================

void test_pops_and_fetches_hand_over_headers() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        res.status = 200;
        if (req.path == "/api/v1/fetch") {
            res.set_content(R"({"entries":[{"queue":"orders","partition":"order-1","highWatermark":2,"logStartOffset":0,"records":[)"
                            R"({"headers":{"type":"order.paid"},"offset":0,"transactionId":"t1","payload":{"n":1},"ts":"2026-11-12T10:00:00.000000Z"},)"
                            R"({"offset":1,"transactionId":"t2","payload":{"n":2},"ts":"2026-11-12T10:00:01.000000Z"}]}]})",
                            "application/json");
        } else {
            res.set_content(POP_WITH_HEADERS, "application/json");
        }
    });
    QueenClient client({server.url()}, config_with());

    const auto messages = client.queue("orders").group("billing").batch(2).wait(false).pop();
    check(messages.size() == 2, "two messages");
    if (messages.size() != 2) return;
    check(messages[0].headers == Headers({{"type", "order.paid"}, {"traceparent", "00-abc-01"}}),
          "the first message's headers");
    check(messages[1].headers.empty(), "the second has none");
    check(messages[0].to_json()["headers"] == json({{"type", "order.paid"}, {"traceparent", "00-abc-01"}}) &&
              !messages[1].to_json().contains("headers"),
          "to_json: headers only when the message has some");

    const auto read = client.queue("orders").fetch({"order-1"});
    const auto& records = read.at("order-1").records;
    check(records.size() == 2, "two records");
    if (records.size() != 2) return;
    check(records[0].headers == Headers({{"type", "order.paid"}}) && records[1].headers.empty(),
          "a fetched record's headers");
}

void test_a_framed_pop_reads_a_frame_with_headers_and_refuses_a_link() {
    const std::string block = block_of({{"type", "order.paid"}});
    json got = wire::decode_pop_frames(framed_answer({
        stored_frame(1, 2 | 8, "t-10", R"({"n":10})", "svc", block),
        stored_frame(2, 0, "t-11", R"({"n":11})")}));
    const json& m = got["messages"];
    check(m.size() == 2, "two messages");
    if (m.size() != 2) return;
    check(m[0]["headers"] == json({{"type", "order.paid"}}) && m[0]["producerSub"] == "svc" &&
              m[0]["data"] == json({{"n", 10}}),
          "the headers after the producer, the payload after them: " + m[0].dump());
    check(!m[1].contains("headers"), "a frame without flag 8 has none: " + m[1].dump());
    const auto messages = util::messages_of(got);
    check(messages.size() == 2 && messages[0].headers == Headers({{"type", "order.paid"}}),
          "and the Message carries them");
    // A filtered group's echo travels in the answer's own keys, and comes
    // through with them.
    check(wire::decode_pop_frames(framed_answer({stored_frame(1, 0, "t-10", R"({"n":10})")},
                                                R"({"frames":true,"filtered":true})"))["filtered"] == true,
          "the filtered echo of a framed answer");

    std::string error;
    try {
        wire::decode_pop_frames(framed_answer({stored_frame(1, 16, "t-10", std::string(24, '\0'))}));
    } catch (const Error& e) {
        error = e.what();
    }
    check(error.find("link") != std::string::npos, "a link frame fails the answer: '" + error + "'");
}

// ============================================================================
// Filtered groups
// ============================================================================

void test_a_filtered_group_sends_its_where_as_a_list() {
    StubServer server(filtered_empty());
    QueenClient client({server.url()}, config_with());

    client.queue("order-events").group("billing").subscription_mode("all").wait(false)
        .where("data.kind", "in", {"order.paid", "order.refunded"})
        .where("headers.type", "=", "order")
        .where("headers.traceparent", "exists")
        .where("data.total", ">", 100)
        .pop();

    const auto pops = server.pops();
    check(pops.size() == 1, "one pop");
    if (pops.size() != 1) return;
    check(pops[0].path == "/api/v1/pop/queue/order-events" && pops[0].param("consumerGroup") == "billing" &&
              pops[0].param("subscriptionMode") == "all",
          "the group's pop: " + pops[0].target);
    const json where = json::parse(pops[0].param("where"));
    check(where == json::array({{{"field", "data.kind"}, {"op", "in"}, {"value", {"order.paid", "order.refunded"}}},
                                {{"field", "headers.type"}, {"op", "="}, {"value", "order"}},
                                {{"field", "headers.traceparent"}, {"op", "exists"}},
                                {{"field", "data.total"}, {"op", ">"}, {"value", 100}}}),
          "the conditions as one JSON list, `exists` without a value: " + where.dump());
}

void test_a_filtered_group_sends_its_where_as_one_string() {
    StubServer server(filtered_empty());
    QueenClient client({server.url()}, config_with());

    client.queue("order-events").partition("order-1").group("billing").wait(false)
        .where("data.kind in ('order.paid', 'order.refunded')")
        .where(std::string("data.total > 100 and headers.note = 'a&b=c+d'"))
        .pop();
    client.queue("order-events").group("audit").wait(false)
        .where(Where("data.kind", "=", "order.paid").add("partition", "prefix", "order-"))
        .pop();

    const auto pops = server.pops();
    check(pops.size() == 2, "two pops");
    if (pops.size() != 2) return;
    check(pops[0].path == "/api/v1/pop/queue/order-events/partition/order-1",
          "the pinned partition route takes it too: " + pops[0].path);
    check(pops[0].param("where") ==
              "data.kind in ('order.paid', 'order.refunded') and data.total > 100 and headers.note = 'a&b=c+d'",
          "two strings, ANDed, arrive as written: " + pops[0].param("where"));
    check(json::parse(pops[1].param("where")) ==
              json::array({{{"field", "data.kind"}, {"op", "="}, {"value", "order.paid"}},
                           {{"field", "partition"}, {"op", "prefix"}, {"value", "order-"}}}),
          "a Where of conditions: " + pops[1].param("where"));
}

void test_every_pop_of_a_consume_loop_sends_the_same_where() {
    StubServer server(filtered_empty());
    QueenClient client({server.url()}, config_with());
    StopAfter safety(10000);

    ConsumeSummary summary = client.queue("orders").group("billing").concurrency(2)
        .wait(false).idle_millis(450).cancel(safety.token())
        .where("data.country", "=", "IT")
        .consume([](const Message&) {});

    check(summary.stopped_by == StopReason::Idle, "the loop stopped idle");
    const auto pops = server.pops();
    check(pops.size() >= 4, "several pops (" + std::to_string(pops.size()) + ")");
    const std::string want = R"([{"field":"data.country","op":"=","value":"IT"}])";
    for (const auto& pop : pops) {
        check(pop.param("where") == want && pop.param("consumerGroup") == "billing",
              "every pop names the group and the same where: " + pop.target);
    }
}

void test_a_pop_without_where_sends_none() {
    StubServer server(nothing());
    QueenClient client({server.url()}, config_with());

    client.queue("orders").group("billing").wait(false).pop();
    client.queue("orders").group("billing").wait(false).where(Where()).pop();

    const auto pops = server.pops();
    check(pops.size() == 2, "two pops");
    for (const auto& pop : pops) check(!pop.has_param("where"), "no where parameter: " + pop.target);
}

void test_where_misuse_is_refused_before_anything_is_sent() {
    StubServer server(nothing());
    QueenClient client({server.url()}, config_with());

    check(refusal([&] { client.queue("orders").wait(false).where("data.x", "=", 1).pop(); })
                  .find("group") != std::string::npos,
          "a where without a group (queue mode)");
    check(!refusal([&] {
              client.queue("orders").wait(false).where("data.x", "=", 1).consume([](const Message&) {});
          }).empty(),
          "consume() refuses it too");
    check(!refusal([&] {
              client.queue().namespace_name("shop").group("g").wait(false).where("data.x", "=", 1).pop();
          }).empty(),
          "a discovery pop, which would read every message unfiltered");
    check(server.calls().empty(), "nothing sent");

    check(refusal([] { Where("data.x", "~", 1); }).find("unknown op") != std::string::npos, "an unknown op");
    check(!refusal([] { Where("data.x", "in", "a"); }).empty(), "in without an array");
    check(!refusal([] { Where("data.x", "prefix", 7); }).empty(), "prefix without a string");
    check(!refusal([] { Where("data.x", "!prefix", nullptr); }).empty(), "!prefix without a string");
    check(!refusal([] { Where("", "=", 1); }).empty(), "a condition without a field");
    check(!refusal([] { Where("data.x = 1").add("data.y", "=", 2); }).empty(), "a condition after a string");
    check(!refusal([] { Where("data.x", "=", 1).add(Where("data.y = 2")); }).empty(), "a string after conditions");
    check(!refusal([&] { client.queue("orders").group("g").where("data.x", "=", 1).where("data.y = 2"); }).empty(),
          "both forms on one builder");
    check(Where("data.ok", "in", json::array()).to_param() == R"([{"field":"data.ok","op":"in","value":[]}])",
          "an empty `in` is the broker's to judge");
}

void test_a_where_mismatch_is_the_brokers_answer() {
    StubServer server(answer(
        R"({"error":"group billing on orders filters with where \"data.kind = 'a'\"; change it with POST /api/v1/consumer-groups/billing/subscription, or use another group","code":"where_mismatch"})",
        400));
    QueenClient client({server.url()}, config_with());

    int status = 0;
    std::string code;
    try {
        client.queue("orders").group("billing").wait(false).where("data.kind = 'b'").pop();
    } catch (const HttpError& e) {
        status = e.status_code();
        code = e.code();
    }
    check(status == 400 && code == "where_mismatch", "pop() throws it: " + std::to_string(status) + " " + code);
}

void test_a_filtered_pop_needs_the_brokers_filtered_echo() {
    // An older broker ignores the where and says nothing: an idle queue is a
    // bodiless 204, a busy one every message, and no answer says it filtered.
    const std::vector<Responder> old_brokers = {
        nothing(), answer(R"({"success":true,"messages":[]})"), answer(POP_WITH_HEADERS),
        answer(R"({"success":true,"messages":[],"filtered":false})")};
    for (size_t i = 0; i < old_brokers.size(); ++i) {
        StubServer server(old_brokers[i]);
        QueenClient client({server.url()}, config_with());
        const std::string error = unsupported([&] {
            client.queue("orders").group("billing").wait(false).where("data.kind = 'paid'").pop();
        });
        check(error.find("does not filter consumer groups; upgrade it") != std::string::npos,
              "old answer " + std::to_string(i) + " fails the pop: '" + error + "'");
    }

    // A broker that filters says so on every answer, empty or not.
    json filtered = json::parse(POP_WITH_HEADERS);
    filtered["filtered"] = true;
    StubServer server(answer(filtered.dump()));
    QueenClient client({server.url()}, config_with());
    const auto messages =
        client.queue("orders").group("billing").wait(false).where("data.kind = 'paid'").pop();
    check(messages.size() == 2, "the filtered answer's messages");

    // A pop without a where expects no echo: its 204 is an empty queue.
    StubServer plain(nothing());
    QueenClient other({plain.url()}, config_with());
    check(other.queue("orders").group("billing").wait(false).pop().empty(), "an unfiltered pop of nothing");
}

void test_a_consume_loop_stops_on_a_broker_that_does_not_filter() {
    StubServer server(answer(POP_WITH_HEADERS));  // every message, and no echo
    QueenClient client({server.url()}, config_with());
    StopAfter safety(10000);
    std::atomic<int> handled{0};

    const std::string error = unsupported([&] {
        client.queue("orders").group("billing").concurrency(2).wait(false).cancel(safety.token())
            .where("data.kind", "=", "paid")
            .consume([&](const Message&) { handled++; });
    });

    check(error.find("does not filter consumer groups") != std::string::npos,
          "consume() throws it: '" + error + "'");
    check(handled.load() == 0, "no message reached the handler");
    const auto pops = server.pops();
    check(!pops.empty() && pops.size() <= 2, "a pop per worker at most: " + std::to_string(pops.size()));
    check(server.calls().size() == pops.size(), "and nothing acked");
}

void test_a_framed_pop_carries_the_filtered_echo_in_its_own_keys() {
    const std::vector<std::string> frames = {stored_frame(1, 0, "t-10", R"({"n":10})")};
    for (bool echoed : {true, false}) {
        StubServer server(answer(framed_answer(frames, echoed ? R"({"frames":true,"filtered":true})"
                                                              : R"({"frames":true})"),
                                 200, wire::POP_FRAMES_CONTENT_TYPE));
        QueenClient client({server.url()}, config_with(false, false, true));
        std::vector<Message> messages;
        const std::string error = unsupported([&] {
            messages = client.queue("orders").group("billing").wait(false).where("data.n > 1").pop();
        });
        if (echoed) {
            check(error.empty() && messages.size() == 1, "with the echo: the message ('" + error + "')");
        } else {
            check(error.find("does not filter") != std::string::npos, "without it: refused ('" + error + "')");
        }
        const auto pops = server.pops();
        check(pops.size() == 1 && pops[0].param("frames") == "true" && pops[0].has_param("where"),
              "a framed pop with its where");
    }
}

void test_set_subscription_where_posts_the_queue_and_the_where() {
    StubServer server(answer(R"({"success":true,"where":"set"})"));
    QueenClient client({server.url()}, config_with());
    Admin admin = client.admin();

    admin.set_subscription_where("billing", "orders", Where("data.kind", "in", {"a", "b"}));
    admin.set_subscription_where("billing", "orders", "data.kind = 'c'");
    admin.set_subscription_where("billing", "orders", Where());

    const auto sent = server.calls();
    check(sent.size() == 3, "three requests");
    if (sent.size() != 3) return;
    for (const auto& call : sent) {
        check(call.method == "POST" && call.path == "/api/v1/consumer-groups/billing/subscription",
              "POST the group's subscription: " + call.method + " " + call.path);
    }
    check(json::parse(sent[0].body) ==
              json({{"queue", "orders"},
                    {"where", json::array({{{"field", "data.kind"}, {"op", "in"}, {"value", {"a", "b"}}}})}}),
          "the list: " + sent[0].body);
    check(json::parse(sent[1].body) == json({{"queue", "orders"}, {"where", "data.kind = 'c'"}}),
          "the string: " + sent[1].body);
    check(json::parse(sent[2].body) == json({{"queue", "orders"}, {"where", nullptr}}),
          "no condition: null, every message: " + sent[2].body);
}

// ============================================================================
// Reading queues
// ============================================================================

void test_a_reading_queue_sends_its_route_and_nothing_its_sources_decide() {
    StubServer server(answer(R"({"queue":"orders-it","route":{"from":[{"queue":"orders"}]},"catchingUp":true})"));
    QueenClient client({server.url()}, config_with());

    QueueConfig cfg;
    cfg.from = {"orders"};
    cfg.where = "data.country = 'IT'";
    cfg.partition_by = "data.customerId";
    cfg.start = "all";
    cfg.lease_time = 30;
    cfg.retry_limit = 5;
    cfg.retry_delay = 1000;
    cfg.dead_letter_queue = true;
    cfg.dlq_after_max_retries = true;
    cfg.priority = 0;
    cfg.delayed_processing = 0;
    cfg.window_buffer = 0;
    json out = client.queue("orders-it").config(cfg).create();

    check(out["catchingUp"] == true, "the broker's answer comes back as it is");
    const auto sent = server.configures();
    check(sent.size() == 1, "one configure");
    if (sent.size() != 1) return;
    const json body = json::parse(sent[0].body);
    check(body == json({{"queue", "orders-it"},
                        {"options", {{"from", {"orders"}},
                                     {"where", "data.country = 'IT'"},
                                     {"partitionBy", "data.customerId"},
                                     {"start", "all"},
                                     {"leaseTime", 30},
                                     {"retryLimit", 5},
                                     {"retryDelay", 1000},
                                     {"deadLetterQueue", true},
                                     {"dlqAfterMaxRetries", true},
                                     {"priority", 0},
                                     {"delayedProcessing", 0},
                                     {"windowBuffer", 0}}}}),
          "the body: " + body.dump());
    for (const char* key : {"retentionEnabled", "retentionSeconds", "completedRetentionSeconds",
                            "maxWaitTimeSeconds", "dedupWindowSeconds", "encryptionEnabled", "maxSize",
                            "retentionSinkHold", "retentionSinkHoldMaxSeconds"}) {
        check(!body["options"].contains(key), std::string("no ") + key + ": its sources decide it");
    }
}

void test_a_reading_queues_sources_and_where_take_every_form() {
    StubServer server(answer(R"({"queue":"r","route":{"from":[{"queue":"orders"}]}})"));
    QueenClient client({server.url()}, config_with());

    QueueConfig cfg;
    cfg.from = {{"orders", Where("data.country", "=", "IT")}, "refunds", {"returns", "data.total > 10"}};
    cfg.where = Where("data.kind", "in", {"a", "b"});
    client.queue("r").configure(cfg);

    QueueConfig drop_where;
    drop_where.where = Where();         // an existing reader reads every message again
    drop_where.retention_seconds = 60;  // set on purpose: sent, and the broker's to refuse
    client.queue("r").configure(drop_where);

    const auto sent = server.configures();
    check(sent.size() == 2, "two configures");
    if (sent.size() != 2) return;
    const json first = json::parse(sent[0].body)["options"];
    check(first["from"] == json::array({{{"queue", "orders"},
                                         {"where", json::array({{{"field", "data.country"}, {"op", "="}, {"value", "IT"}}})}},
                                        "refunds",
                                        {{"queue", "returns"}, {"where", "data.total > 10"}}}),
          "a source by name, or with its own where in either form: " + first["from"].dump());
    check(first["where"] == json::array({{{"field", "data.kind"}, {"op", "in"}, {"value", {"a", "b"}}}}),
          "the where of the sources named alone: " + first["where"].dump());
    const json second = json::parse(sent[1].body)["options"];
    check(second == json({{"where", nullptr}, {"retentionSeconds", 60}}),
          "an empty where is null, and an option the caller set is sent: " + second.dump());
}

void test_stop_reading_sends_from_null() {
    StubServer server(answer(R"({"queue":"r"})"));
    QueenClient client({server.url()}, config_with());

    QueueConfig stop;
    stop.stop_reading = true;
    client.queue("orders-it").configure(stop);

    const auto sent = server.configures();
    check(sent.size() == 1 && json::parse(sent[0].body)["options"] == json({{"from", nullptr}}),
          "from: null and nothing else: " + (sent.empty() ? std::string() : sent[0].body));

    QueueConfig both;
    both.stop_reading = true;
    both.from = {"orders"};
    check(!refusal([&] { client.queue("orders-it").configure(both); }).empty(), "stop_reading with from");
    check(server.configures().size() == 1, "and that one is not sent");
}

void test_a_reading_queue_needs_the_brokers_route() {
    // An older broker takes `from` for an unknown option and answers as it
    // does for any queue: no route.
    StubServer server(answer(R"({"configured":true,"queue":"orders-it","options":{}})"));
    QueenClient client({server.url()}, config_with());

    QueueConfig italy;
    italy.from = {"orders"};
    italy.where = "data.country = 'IT'";
    const std::string error = unsupported([&] { client.queue("orders-it").configure(italy); });
    check(error.find("does not read other queues; upgrade it") != std::string::npos,
          "configure with from: '" + error + "'");

    // Without from there is no route to expect.
    QueueConfig jobs;
    jobs.lease_time = 30;
    check(unsupported([&] { client.queue("jobs").configure(jobs); }).empty(), "an ordinary configure");
    QueueConfig stop;
    stop.stop_reading = true;
    check(unsupported([&] { client.queue("orders-it").configure(stop); }).empty(), "stop_reading");
    check(server.configures().size() == 3, "all three were sent");
}

void test_deleting_a_source_with_force() {
    StubServer server(answer(R"({"deleted":true})"));
    QueenClient client({server.url()}, config_with());

    client.queue("orders").del();
    client.queue("orders").del(true);

    const auto sent = server.calls();
    check(sent.size() == 2, "two deletes");
    if (sent.size() != 2) return;
    check(sent[0].method == "DELETE" && sent[0].path == "/api/v1/resources/queues/orders" &&
              !sent[0].has_param("force"),
          "del(): no force: " + sent[0].target);
    check(sent[1].method == "DELETE" && sent[1].param("force") == "true", "del(true): force=true: " + sent[1].target);
}

// ============================================================================
// link
// ============================================================================

void test_link_is_one_op_per_message_named_as_its_pop_returned_it() {
    StubServer server(committed(json::array({{{"index", 0}}, {{"index", 1}}, {{"index", 2}}})));
    QueenClient client({server.url()}, config_with());
    const auto messages = popped();

    TransactionResult result = client.transaction()
        .ack(messages[0])
        .queue("manual-review").partition("order-1").link(messages, {{"review-reason", "amount"}})
        .commit();

    check(result.success, "committed");
    const auto sent = server.transactions();
    check(sent.size() == 1, "one transaction");
    if (sent.size() != 1) return;
    const json ops = json::parse(sent[0].body)["operations"];
    check(ops.size() == 3 && ops[0]["type"] == "ack", "the ack, then one link per message: " + ops.dump());
    if (ops.size() != 3) return;
    check(ops[1] == json({{"type", "link"},
                          {"queue", "manual-review"},
                          {"partition", "order-1"},
                          {"from", {{"partitionId", "7"}, {"offset", 42}, {"transactionId", "t1"},
                                    {"consumerGroup", "billing"}}},
                          {"headers", {{"review-reason", "amount"}}}}),
          "the first link: " + ops[1].dump());
    check(ops[2]["from"] == json({{"partitionId", "7"}, {"offset", 43}, {"transactionId", "t2"},
                                  {"consumerGroup", "billing"}}),
          "the second names its own message: " + ops[2]["from"].dump());
}

void test_a_link_sends_only_what_it_has() {
    StubServer server(committed(json::array({{{"index", 0}}, {{"index", 1}}})));
    QueenClient client({server.url()}, config_with());

    // Queue mode: the pop named no group, and an ack would send none either.
    Message queue_mode = popped()[0];
    queue_mode.consumer_group = util::QUEUE_MODE_GROUP;
    // A feed entry's partition id is a UUID, not a pop's: named by queue and partition.
    Message feed_entry = popped()[1];
    feed_entry.partition_id = "0190aaaa-0000-7000-8000-000000000007";
    feed_entry.consumer_group.clear();

    client.transaction().queue("audit").link(queue_mode).queue("audit").link(feed_entry).commit();

    const auto sent = server.transactions();
    check(sent.size() == 1, "one transaction");
    if (sent.size() != 1) return;
    const json ops = json::parse(sent[0].body)["operations"];
    check(ops.size() == 2, "two links");
    if (ops.size() != 2) return;
    check(ops[0] == json({{"type", "link"},
                          {"queue", "audit"},
                          {"from", {{"partitionId", "7"}, {"offset", 42}, {"transactionId", "t1"}}}}),
          "no partition, no headers, no group in queue mode: " + ops[0].dump());
    check(ops[1]["from"] == json({{"queue", "orders"}, {"partition", "order-1"}, {"offset", 43},
                                  {"transactionId", "t2"}}),
          "a feed entry by queue and partition: " + ops[1]["from"].dump());
}

void test_a_message_that_cannot_be_named_is_refused_before_anything_is_sent() {
    StubServer server(committed(json::array({{{"index", 0}}})));
    QueenClient client({server.url()}, config_with());
    const Message good = popped()[0];

    Message no_offset = good;
    no_offset.offset.reset();
    Message no_txn = good;
    no_txn.transaction_id.clear();
    Message nowhere = good;
    nowhere.partition_id.clear();
    nowhere.queue.clear();

    check(refusal([&] { client.transaction().queue("r").link(no_offset); }).find("offset") != std::string::npos,
          "a message without its offset");
    check(!refusal([&] { client.transaction().queue("r").link(no_txn); }).empty(),
          "a message without its transaction id");
    check(!refusal([&] { client.transaction().queue("r").link(nowhere); }).empty(),
          "a message with no partition id, nor a queue and a partition");
    check(!refusal([&] { client.transaction().queue("").link(good); }).empty(), "no target queue");

    // A refused message leaves the bundle as it was: none of its batch is added.
    TransactionBuilder tx = client.transaction();
    tx.queue("r").link(good);
    check(!refusal([&] { tx.queue("r").link(std::vector<Message>{good, no_offset}); }).empty(),
          "a batch with one message that cannot be named");
    check(server.transactions().empty(), "nothing sent");
    tx.commit();
    const auto sent = server.transactions();
    check(sent.size() == 1 && json::parse(sent[0].body)["operations"].size() == 1,
          "the bundle holds the one link added before");
}

void test_a_link_the_broker_rolls_back_throws_transaction_error() {
    const json rolled_back = {{"transactionId", "tx"}, {"success", false}, {"reason", "source_gone"},
                              {"error", "source_gone: offset 42 of orders is not retained"},
                              {"results", json::array()}};
    StubServer server(answer(rolled_back.dump()));
    QueenClient client({server.url()}, config_with());

    std::string reason;
    try {
        client.transaction().ack(popped()[0]).queue("review").link(popped()[0]).commit();
    } catch (const TransactionError& e) {
        reason = e.result().reason;
    }
    check(reason == "source_gone", "TransactionError with the broker's reason: '" + reason + "'");
}

// ============================================================================

int main() {
    std::cout << BLUE << "Queen C++ client - headers, filtered groups, reading queues and link wire tests"
              << (wire::zstd_available() ? " (with zstd)" : " (without zstd)") << RESET << std::endl
              << std::endl;

    run_test("a push carries headers only when it has some", test_a_push_carries_headers_only_when_it_has_some);
    run_test("a buffered push keeps each item's headers", test_a_buffered_push_keeps_each_items_headers);
    run_test("a header name the broker refuses is refused before anything is sent",
             test_a_header_name_the_broker_refuses_is_refused_before_anything_is_sent);
    run_test("encode_headers writes the block in name order", test_encode_headers_writes_the_block_in_name_order);
    run_test("the plain binary push leaves a push with headers to JSON",
             test_the_plain_binary_push_leaves_a_push_with_headers_to_json);
    run_zstd_test("a framed push carries headers in its frames", test_a_framed_push_carries_headers_in_its_frames);
    run_test("a transaction push carries headers", test_a_transaction_push_carries_headers);
    run_zstd_test("a binary transaction carries headers in its frames and links in its head",
                  test_a_binary_transaction_carries_headers_in_its_frames_and_links_in_its_head);
    run_test("a timer carries headers, standalone and as a rider",
             test_a_timer_carries_headers_standalone_and_as_a_rider);
    run_test("pops and fetches hand over headers", test_pops_and_fetches_hand_over_headers);
    run_test("a framed pop reads a frame with headers and refuses a link",
             test_a_framed_pop_reads_a_frame_with_headers_and_refuses_a_link);

    run_test("a filtered group sends its where as a list", test_a_filtered_group_sends_its_where_as_a_list);
    run_test("a filtered group sends its where as one string", test_a_filtered_group_sends_its_where_as_one_string);
    run_test("every pop of a consume loop sends the same where",
             test_every_pop_of_a_consume_loop_sends_the_same_where);
    run_test("a pop without where sends none", test_a_pop_without_where_sends_none);
    run_test("where misuse is refused before anything is sent",
             test_where_misuse_is_refused_before_anything_is_sent);
    run_test("a where_mismatch is the broker's answer", test_a_where_mismatch_is_the_brokers_answer);
    run_test("a filtered pop needs the broker's filtered echo",
             test_a_filtered_pop_needs_the_brokers_filtered_echo);
    run_test("a consume loop stops on a broker that does not filter",
             test_a_consume_loop_stops_on_a_broker_that_does_not_filter);
    run_zstd_test("a framed pop carries the filtered echo in its own keys",
                  test_a_framed_pop_carries_the_filtered_echo_in_its_own_keys);
    run_test("set_subscription_where posts the queue and the where",
             test_set_subscription_where_posts_the_queue_and_the_where);

    run_test("a reading queue sends its route and nothing its sources decide",
             test_a_reading_queue_sends_its_route_and_nothing_its_sources_decide);
    run_test("a reading queue's sources and where take every form",
             test_a_reading_queues_sources_and_where_take_every_form);
    run_test("stop_reading sends from: null", test_stop_reading_sends_from_null);
    run_test("a reading queue needs the broker's route", test_a_reading_queue_needs_the_brokers_route);
    run_test("deleting a source, with force", test_deleting_a_source_with_force);

    run_test("link is one op per message, named as its pop returned it",
             test_link_is_one_op_per_message_named_as_its_pop_returned_it);
    run_test("a link sends only what it has", test_a_link_sends_only_what_it_has);
    run_test("a message that cannot be named is refused before anything is sent",
             test_a_message_that_cannot_be_named_is_refused_before_anything_is_sent);
    run_test("a link the broker rolls back throws TransactionError",
             test_a_link_the_broker_rolls_back_throws_transaction_error);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All headers, filtered group, reading queue and link wire tests passed" << RESET;
        if (skipped > 0) std::cout << YELLOW << " (" << skipped << " skipped: built without zstd)" << RESET;
        std::cout << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
