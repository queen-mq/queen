// docs:start(app-cpp-chat)
//
// A chat messaging system.
//
// This is the application Queen was written for. A hotel messaging product ran
// on Kafka and kept stalling: some conversations need a translation or an agent
// reply before the next message can be handled, and on a shared partition one
// slow conversation holds up every conversation behind it.
//
// The fix is structural rather than operational: one ordered lane per
// conversation, created by the first message sent to it. A conversation that
// takes ten seconds delays itself and nothing else.
//
// What this program builds:
//
//   chat-messages (one partition per conversation)
//     |-- group "delivery"    fast, marks each message as delivered
//     `-- group "enrichment"  slow on conversations that need translation
//
// And what it proves: every message reaches both groups exactly once, in the
// order it was sent inside its own conversation, and the conversations that
// need no translation finish while the slow one is still working.
//
// Build it (see examples/tutorials/cpp/01-hello-world.cpp for the headers
// queen_client.hpp expects but this repository does not vendor -- json.hpp
// under clients/server/vendor, threadpool.hpp under clients/server/include --
// and for why -lssl -lcrypto is required even over plain http):
//   mkdir -p build
//   c++ -std=c++17 -O1 -pthread \
//       -I../../../clients/client-cpp -I../../../clients/server/vendor \
//       -I/opt/homebrew/include -I"$(brew --prefix openssl)/include" \
//       chat.cpp -o build/chat \
//       -L"$(brew --prefix openssl)/lib" -lssl -lcrypto -lpthread
//
// Run it:
//   QUEEN_URL=http://localhost:6632 ./build/chat

#include "queen_client.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using queen::QueenClient;
using queen::QueueBuilder;
using json = nlohmann::json;

// The queue name is prefixed per language and suffixed per run, so every
// application in every language can share one broker and no run inherits state
// from another.
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

struct Conversation {
    std::string id;
    std::string locale;
    bool needs_translation;
};

// Three conversations. The one in Japanese needs a translation pass, which is
// the slow work: 400 ms a message against 10 ms for the rest.
static const std::vector<Conversation> CONVERSATIONS = {
    {"conv-en-1", "en", false},
    {"conv-en-2", "en", false},
    {"conv-jp-1", "jp", true},
};
static const int MESSAGES_PER_CONVERSATION = 6;
static const int TOTAL_MESSAGES =
    MESSAGES_PER_CONVERSATION * static_cast<int>(CONVERSATIONS.size());

static int checks = 0;

// C++ has no assert that survives -DNDEBUG and carries a message, so this is a
// throwing check: the failure travels to main() as an exception, which is what
// turns it into "FAIL: <reason>" and a non-zero exit.
static void check(bool condition, const std::string& description) {
    if (!condition) throw std::runtime_error(description);
    ++checks;
    std::cout << "  ok: " << description << std::endl;
}

static const Conversation& conversation_by_id(const std::string& id) {
    for (const Conversation& c : CONVERSATIONS) {
        if (c.id == id) return c;
    }
    throw std::runtime_error("unknown conversation " + id);
}

static void sleep_millis(int millis) {
    std::this_thread::sleep_for(std::chrono::milliseconds(millis));
}

static std::string join(const std::vector<int>& values) {
    std::ostringstream out;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i) out << ", ";
        out << values[i];
    }
    return out.str();
}

static long long millis_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start)
        .count();
}

int main() {
    const char* env_url = std::getenv("QUEEN_URL");
    const std::string QUEEN_URL = env_url ? env_url : "http://localhost:6632";
    const std::string MESSAGES = "app-cpp-chat-" + run_id();

    // Unlike the JavaScript client there is no handleSignals switch:
    // QueenClient always installs its own SIGINT/SIGTERM handlers, so a Ctrl-C
    // during a run is the client's exit, not yours.
    QueenClient client(QUEEN_URL);

    std::string verdict;
    bool failed = false;

    try {
        std::cout << "broker " << QUEEN_URL << std::endl;

        // Leases are what make a crashed worker safe: a message whose handler
        // dies is redelivered once the lease expires. retry_limit bounds how
        // many times that can happen before the message is dead-lettered
        // instead. The C++ QueueConfig is a struct rather than an option bag,
        // and every field it does not carry keeps the broker's own default.
        queen::QueueConfig config;
        config.lease_time = 60;
        config.retry_limit = 3;
        client.queue(MESSAGES).config(config).create();

        // ------------------------------------------------------------ producing
        //
        // A chat client sends a message: one push, into the partition named
        // after the conversation. Nothing was declared for this conversation in
        // advance, and nothing has to be cleaned up when it goes quiet.
        std::cout << "\nsending" << std::endl;
        int sent = 0;
        for (int seq = 1; seq <= MESSAGES_PER_CONVERSATION; ++seq) {
            for (const Conversation& conv : CONVERSATIONS) {
                // push() takes a vector of items because one call can carry a
                // batch; the broker answers with one result per item, in order.
                //
                // The transaction id is the client's own idempotency key: a
                // retry of this send, from a phone on a flaky network, writes
                // nothing the second time and answers with the first message's
                // id.
                client.queue(MESSAGES).partition(conv.id).push({
                    json{{"transactionId", conv.id + "-" + std::to_string(seq)},
                         {"data", {{"conversationId", conv.id},
                                   {"seq", seq},
                                   {"locale", conv.locale},
                                   {"body", "message " + std::to_string(seq) +
                                                " in " + conv.id}}}}
                });
                ++sent;
            }
        }
        std::cout << "  " << sent << " messages across " << CONVERSATIONS.size()
                  << " conversations" << std::endl;

        // A resend of the same message: the client retried because it never saw
        // the first answer. The broker recognises the transaction id and stores
        // nothing. push() hands back the broker's reply unwrapped, so the
        // per-item verdict is read straight off the array.
        json duplicate = client.queue(MESSAGES).partition("conv-en-1").push({
            json{{"transactionId", "conv-en-1-1"},
                 {"data", {{"conversationId", "conv-en-1"},
                           {"seq", 1},
                           {"body", "resent by the phone"}}}}
        });
        check(duplicate.is_array() && duplicate.size() == 1 &&
                  duplicate[0]["status"] == "duplicate",
              "a resent message was deduplicated, not stored twice");

        // ----------------------------------------------------------- delivering
        //
        // The delivery worker is what marks a message as delivered to the
        // recipients. It is fast and must never fall behind, which is why it is
        // its own consumer group: it shares no cursor with the slow work below.
        //
        // concurrency(3) runs three poll loops, and each pop claims a
        // partition, so the three conversations are drained in parallel by
        // three workers. Nothing raises partitions() here: one lane per worker
        // is exactly the point.
        //
        // Three settings keep that loop honest:
        //
        //   wait(false)     a long-polling pop parks server-side for up to 30
        //                   seconds and the idle clock is only consulted
        //                   between polls, so a blocking pop would stretch a 10
        //                   second idle budget into half a minute.
        //   idle_millis     the deadline: a lost message fails this run instead
        //                   of hanging it.
        //   limit()         counts per worker, here and in the JavaScript
        //                   alike: each worker keeps its own tally, so three
        //                   workers sharing 18 messages never see one worker
        //                   reach 18. It is a backstop, not the thing that ends
        //                   the run.
        //
        // What ends the run is the shared counter below, which raises the stop
        // flag the moment every message has been handled. Without it every
        // consume in this file would sit out its idle deadline after the last
        // message, which is exactly what the JavaScript pays.
        //
        // consume() is synchronous in C++: it blocks this thread and runs
        // concurrency() workers in a pool until the stop flag, the idle
        // deadline or a worker's own limit ends every one of them. It also
        // catches whatever the handler throws and turns it into a
        // negative acknowledgement, so an exception raised in there never
        // reaches main() on its own: record it, raise the stop flag, and
        // rethrow once consume() has returned.
        std::cout << "\ndelivering" << std::endl;
        std::mutex lock;
        std::map<std::string, std::vector<int>> delivered;
        std::atomic<int> handled{0};
        std::atomic<bool> stop{false};
        std::exception_ptr handler_error;

        client.queue(MESSAGES)
            .group("delivery")
            .subscription_mode("all")
            .concurrency(3)
            .each()
            .limit(TOTAL_MESSAGES)
            .wait(false)
            .idle_millis(10000)
            .consume([&](const json& msg) {
                try {
                    sleep_millis(10);
                    {
                        std::lock_guard<std::mutex> guard(lock);
                        delivered[msg["data"]["conversationId"].get<std::string>()]
                            .push_back(msg["data"]["seq"].get<int>());
                    }
                    // The acknowledgement of this message happens after the
                    // handler returns, so the stop flag raised here still lets
                    // the last message commit.
                    if (++handled >= TOTAL_MESSAGES) stop = true;
                } catch (...) {
                    std::lock_guard<std::mutex> guard(lock);
                    if (!handler_error) handler_error = std::current_exception();
                    stop = true;
                }
            }, &stop);
        if (handler_error) std::rethrow_exception(handler_error);

        int delivered_total = 0;
        for (const auto& entry : delivered) delivered_total += entry.second.size();
        check(delivered_total == sent, "delivery saw every message exactly once");

        // One lane is only ever held by one worker at a time -- the lease is
        // released by the acknowledgement -- so the sequence numbers inside a
        // conversation come back in the order they were sent, however many
        // workers are running.
        std::vector<int> in_order;
        for (int seq = 1; seq <= MESSAGES_PER_CONVERSATION; ++seq) in_order.push_back(seq);
        for (const Conversation& conv : CONVERSATIONS) {
            check(delivered[conv.id] == in_order, conv.id + " was delivered in order");
        }

        // ----------------------------------------------------------- enrichment
        //
        // The slow group. It reads the same messages through its own cursor,
        // and the Japanese conversation costs 400 ms a message because it has
        // to be translated before it can be answered.
        //
        // This is where a shared partition would hurt: on a hashed topic these
        // messages would sit in the same lane as the English ones and hold them
        // up. Here each conversation has its own lane, so the English
        // conversations finish while the Japanese one is still being
        // translated. The timings below are the proof.
        std::cout << "\nenriching" << std::endl;
        std::map<std::string, long long> finished_at;
        handled = 0;
        stop = false;
        auto started = std::chrono::steady_clock::now();

        client.queue(MESSAGES)
            .group("enrichment")
            .subscription_mode("all")
            .concurrency(3)
            .each()
            .limit(TOTAL_MESSAGES)
            .wait(false)
            .idle_millis(15000)
            .consume([&](const json& msg) {
                try {
                    const std::string id =
                        msg["data"]["conversationId"].get<std::string>();
                    sleep_millis(conversation_by_id(id).needs_translation ? 400 : 10);
                    {
                        std::lock_guard<std::mutex> guard(lock);
                        finished_at[id] = millis_since(started);
                    }
                    if (++handled >= TOTAL_MESSAGES) stop = true;
                } catch (...) {
                    std::lock_guard<std::mutex> guard(lock);
                    if (!handler_error) handler_error = std::current_exception();
                    stop = true;
                }
            }, &stop);
        if (handler_error) std::rethrow_exception(handler_error);

        const long long slow = finished_at["conv-jp-1"];
        const long long fast =
            std::max(finished_at["conv-en-1"], finished_at["conv-en-2"]);
        std::cout << "  english done after " << fast << " ms, japanese after "
                  << slow << " ms" << std::endl;

        check(fast < slow,
              "the conversations needing no translation finished first, in the "
              "same worker pool");
        check(slow > MESSAGES_PER_CONVERSATION * 300,
              "the slow conversation really was slow, so the comparison means "
              "something");

        // --------------------------------------------------------------- replay
        //
        // A new feature needs the history: sentiment scoring over everything
        // ever said. It is a new consumer group reading from the beginning, and
        // it costs no producer change and no second copy of the data.
        //
        // subscription_mode("all") is what points the new cursor at the
        // beginning: the default for a new group is the tail, so without it
        // this group would sit idle waiting for the next message and the run
        // would end on the idle deadline with nothing scored.
        std::cout << "\nbackfilling a new consumer" << std::endl;
        std::atomic<int> scored{0};
        handled = 0;
        stop = false;

        client.queue(MESSAGES)
            .group("sentiment")
            .subscription_mode("all")
            .concurrency(3)
            .each()
            .limit(TOTAL_MESSAGES)
            .wait(false)
            .idle_millis(10000)
            .consume([&](const json&) {
                ++scored;
                if (++handled >= TOTAL_MESSAGES) stop = true;
            }, &stop);

        check(scored.load() == sent, "a group added today read the whole history");

        std::cout << "\n  delivery order in conv-jp-1: "
                  << join(delivered["conv-jp-1"]) << std::endl;

        // Clean up on success only: a failed run leaves the queue on the broker
        // to be looked at. del(), not delete: the word is taken.
        client.queue(MESSAGES).del();

        verdict = "\nPASS: " + std::to_string(checks) + " checks";
    } catch (const std::exception& err) {
        verdict = std::string("\nFAIL: ") + err.what();
        failed = true;
    }

    // close() flushes anything still sitting in the client-side push buffers
    // and drops them along with their timer threads. It narrates its own
    // shutdown on stdout, which is why the verdict is printed after it rather
    // than before: PASS or FAIL stays the last line of a run.
    client.close();

    (failed ? std::cerr : std::cout) << verdict << std::endl;
    return failed ? 1 : 0;
}
// docs:end
