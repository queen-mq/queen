// docs:start(app-cpp-chat)
//
// A chat backend: one ordered partition per conversation.
//
// Queen started as the broker of a hotel messaging product. Some conversations
// need a translation or an agent before their next message can be handled, and
// on a hashed Kafka topic one slow conversation held up every conversation that
// shared its partition. Here every conversation is a partition of its own,
// created by the first message sent to it, so a slow conversation waits on
// itself and on nothing else.
//
//   chat-messages (one partition per conversation)
//     |-- group "delivery"    marks each message delivered, fast
//     |-- group "enrichment"  translates the Japanese conversation, slow
//     `-- group "sentiment"   added later, reads the whole history
//
// The program checks what the design promises: every message reaches each
// group once and in the order of its conversation, and the English
// conversations finish while the Japanese one is still being translated.
//
// Build it. queen_client.hpp carries its nlohmann/json beside it; the one
// header to install is cpp-httplib's (Homebrew puts httplib.h under
// /opt/homebrew/include). The client switches on cpp-httplib's OpenSSL support
// for https://, so -lssl -lcrypto are needed even over plain http.
//   mkdir -p build
//   c++ -std=c++17 -O1 -pthread \
//       -I../../../clients/client-cpp \
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

// Three conversations. The Japanese one needs a translation pass, 400 ms a
// message against 10 ms for the others. It is listed first, so its messages are
// the oldest in the queue and its partition is usually handed out first: a
// consumer that let one conversation hold up another would fail the timing
// check below.
static const std::vector<Conversation> CONVERSATIONS = {
    {"conv-jp-1", "jp", true},
    {"conv-en-1", "en", false},
    {"conv-en-2", "en", false},
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
        if (i) out << ",";
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

    // The client installs no signal handlers unless asked
    // (ClientConfig::handle_signals): a Ctrl-C ends this run as usual.
    queen::Queen client(QUEEN_URL);

    std::string verdict;
    bool failed = false;

    try {
        std::cout << "broker " << QUEEN_URL << std::endl;

        // A crashed worker's messages come back when its lease expires, and
        // retry_limit bounds how often a failing message is retried before it
        // goes to the dead-letter queue. An option left unset keeps the
        // broker's default.
        queen::QueueConfig config;
        config.lease_time = 60;
        config.retry_limit = 3;
        client.queue(MESSAGES).config(config).create();

        // -------------------------------------------------------------- sending
        //
        // Sending a message is one push into the conversation's partition.
        // Nothing was declared for the conversation beforehand, and nothing has
        // to be cleaned up when it goes quiet.
        std::cout << "\nsending" << std::endl;
        int sent = 0;
        for (int seq = 1; seq <= MESSAGES_PER_CONVERSATION; ++seq) {
            for (const Conversation& conv : CONVERSATIONS) {
                // The transaction id is the phone's own id for the message. A
                // phone that retries a send it never saw answered writes nothing
                // the second time, and gets the first message's id back.
                client.queue(MESSAGES).partition(conv.id).push(
                    {{"conversationId", conv.id},
                     {"seq", seq},
                     {"locale", conv.locale},
                     {"body", "message " + std::to_string(seq) + " in " + conv.id}},
                    conv.id + "-" + std::to_string(seq));
                ++sent;
            }
        }
        std::cout << "  " << sent << " messages across " << CONVERSATIONS.size()
                  << " conversations" << std::endl;

        // The phone resends message 1 because the answer got lost on a bad
        // network. push() answers one verdict per message.
        auto duplicate = client.queue(MESSAGES).partition("conv-en-1").push(
            {{"conversationId", "conv-en-1"}, {"seq", 1}, {"body", "resent by the phone"}},
            "conv-en-1-1");
        check(duplicate.size() == 1 && duplicate[0].status == queen::PushStatus::Duplicate,
              "a resent message was recognised and not stored twice");

        // ----------------------------------------------------------- delivering
        //
        // Marking messages delivered is fast work and must never wait behind
        // slow work, so it is a consumer group of its own, with its own cursor.
        //
        // concurrency(3) runs three workers, each a thread with a poll loop of
        // its own. partitions(1) makes every pop take ONE conversation: by
        // default a pop may sweep up several ready conversations, and a worker
        // handles the messages of one pop in order, so a slow conversation
        // would delay the others that came with it.
        //
        // consume() blocks this thread until every worker has stopped, and
        // three settings bound it:
        //
        //   limit()         counts across all the workers: the phase ends once
        //                   every message has been handled.
        //   wait(false)     every pop answers at once, and a worker that found
        //                   nothing sleeps briefly before the next one; the
        //                   idle deadline is consulted between pops.
        //   idle_millis     the deadline: a lost message fails this run instead
        //                   of hanging it.
        //
        // consume() also catches whatever the handler throws and turns it into
        // a negative acknowledgement, so an exception raised in there never
        // reaches main() on its own: record it, cancel the loop, and rethrow
        // once consume() has returned.
        std::cout << "\ndelivering" << std::endl;
        std::mutex lock;
        std::map<std::string, std::vector<int>> delivered;
        queen::Cancel stop;
        std::exception_ptr handler_error;

        client.queue(MESSAGES)
            .group("delivery")
            .subscription_mode("all")
            .concurrency(3)
            .partitions(1)
            .limit(TOTAL_MESSAGES)
            .wait(false)
            .idle_millis(10000)
            .cancel(stop)
            .consume([&](const queen::Message& msg) {
                try {
                    sleep_millis(10);
                    std::lock_guard<std::mutex> guard(lock);
                    delivered[msg.data["conversationId"].get<std::string>()]
                        .push_back(msg.data["seq"].get<int>());
                } catch (...) {
                    std::lock_guard<std::mutex> guard(lock);
                    if (!handler_error) handler_error = std::current_exception();
                    stop.cancel();
                }
            });
        if (handler_error) std::rethrow_exception(handler_error);

        int delivered_total = 0;
        for (const auto& entry : delivered) delivered_total += entry.second.size();
        check(delivered_total == sent,
              "delivery saw all " + std::to_string(sent) + " messages once (got " +
                  std::to_string(delivered_total) + ")");

        // A conversation is leased to one worker at a time, and the lease moves
        // on only when that worker has acknowledged what it took, so the
        // sequence numbers inside a conversation come back in the order they
        // were sent, however many workers are running.
        std::vector<int> in_order;
        for (int seq = 1; seq <= MESSAGES_PER_CONVERSATION; ++seq) in_order.push_back(seq);
        for (const Conversation& conv : CONVERSATIONS) {
            check(delivered[conv.id] == in_order,
                  conv.id + " was delivered in order: " + join(delivered[conv.id]));
        }

        // ----------------------------------------------------------- enrichment
        //
        // The slow group reads the same messages through its own cursor. On a
        // topic with a few hashed partitions, the Japanese conversation would
        // sit in a partition shared with English ones and hold them up. Here
        // each worker holds one conversation at a time, so the English
        // conversations finish while the Japanese one is still being
        // translated.
        std::cout << "\nenriching" << std::endl;
        std::map<std::string, long long> finished_at;
        auto started = std::chrono::steady_clock::now();

        client.queue(MESSAGES)
            .group("enrichment")
            .subscription_mode("all")
            .concurrency(3)
            .partitions(1)
            .limit(TOTAL_MESSAGES)
            .wait(false)
            .idle_millis(15000)
            .cancel(stop)
            .consume([&](const queen::Message& msg) {
                try {
                    const std::string id = msg.data["conversationId"].get<std::string>();
                    sleep_millis(conversation_by_id(id).needs_translation ? 400 : 10);
                    std::lock_guard<std::mutex> guard(lock);
                    finished_at[id] = millis_since(started);
                } catch (...) {
                    std::lock_guard<std::mutex> guard(lock);
                    if (!handler_error) handler_error = std::current_exception();
                    stop.cancel();
                }
            });
        if (handler_error) std::rethrow_exception(handler_error);

        const long long slow = finished_at["conv-jp-1"];
        const long long fast =
            std::max(finished_at["conv-en-1"], finished_at["conv-en-2"]);
        std::cout << "  english done after " << fast << " ms, japanese after "
                  << slow << " ms" << std::endl;

        check(fast < slow,
              "the English conversations finished while the Japanese one was "
              "still being translated");
        check(slow >= MESSAGES_PER_CONVERSATION * 400,
              "the Japanese conversation really took its six translations");

        // ------------------------------------------------------------- backfill
        //
        // A feature added later, sentiment scoring, wants every message ever
        // sent. It is one more consumer group starting from the oldest message:
        // no producer change, and no second copy of the data.
        //
        // subscription_mode("all") is what points the new cursor at the oldest
        // message. A new group starts at the tail by default, so without it this
        // group would wait for the next message, and the phase would end on the
        // idle deadline with nothing scored.
        std::cout << "\nbackfilling a new group" << std::endl;
        std::atomic<int> scored{0};

        client.queue(MESSAGES)
            .group("sentiment")
            .subscription_mode("all")
            .concurrency(3)
            .partitions(1)
            .limit(TOTAL_MESSAGES)
            .wait(false)
            .idle_millis(10000)
            .consume([&](const queen::Message&) { ++scored; });

        check(scored.load() == sent,
              "a group created now read the whole history (" +
                  std::to_string(scored.load()) + " messages)");

        // Clean up on success only: a failed run leaves the queue on the broker
        // to be looked at. The method is del() because delete is a keyword.
        client.queue(MESSAGES).del();

        verdict = "\nPASS: " + std::to_string(checks) + " checks";
    } catch (const std::exception& err) {
        verdict = std::string("\nFAIL: ") + err.what();
        failed = true;
    }

    // close() flushes anything still sitting in the client-side push buffers
    // and closes the pooled connections. The verdict is printed after it, so
    // PASS or FAIL stays the last line of a run.
    client.close();

    (failed ? std::cerr : std::cout) << verdict << std::endl;
    return failed ? 1 : 0;
}
// docs:end
