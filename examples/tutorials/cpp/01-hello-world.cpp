// docs:start(tut-cpp-hello-world)
//
// Tutorial 1 of 4: hello world.
//
// One message in, one message out. Nothing is created in advance: the queue and
// the partition come into existence with the push that names them.
//
// The C++ client is header-only: the whole program is this file plus one
// include. nlohmann/json is vendored beside the header (third_party/); the one
// dependency to install is cpp-httplib (brew install cpp-httplib). The header
// turns cpp-httplib's OpenSSL support on so https:// works, so -lssl -lcrypto
// is needed even over plain http (define QUEEN_CLIENT_NO_TLS to build without).
// With CMake, link the queen::client target instead.
//
//   mkdir -p build
//   g++ -std=c++17 -O2 -pthread \
//       -I../../../clients/client-cpp \
//       -I/opt/homebrew/include -I"$(brew --prefix openssl)/include" \
//       01-hello-world.cpp -o build/01-hello-world \
//       -L"$(brew --prefix openssl)/lib" -lssl -lcrypto -lpthread
//
// Run it:
//   QUEEN_URL=http://localhost:6632 ./build/01-hello-world
//
// The program checks its own outcome and exits non-zero if a check fails.

#include "queen_client.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

// The name is prefixed per language and suffixed per run, so every tutorial in
// every language can share one broker and no run inherits state from another.
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

static int checks = 0;

// C++ has no assert that survives -DNDEBUG and carries a message, so the
// tutorials use a throwing check: the failure travels to main() as an
// exception, which is what turns it into "FAIL: <reason>" and a non-zero exit.
static void check(bool condition, const std::string& description) {
    if (!condition) throw std::runtime_error(description);
    ++checks;
    std::cout << "  ok: " << description << std::endl;
}

int main() {
    const char* env_url = std::getenv("QUEEN_URL");
    const std::string QUEEN_URL = env_url ? env_url : "http://localhost:6632";
    const std::string QUEUE = "tut-cpp-hello-" + run_id();

    // The constructor takes a base URL and opens nothing until the first call.
    // It installs no signal handlers: a Ctrl-C ends the program as usual
    // (ClientConfig::handle_signals opts in to a graceful stop).
    queen::Queen client(QUEEN_URL);

    std::string verdict;
    bool failed = false;

    try {
        std::cout << "broker " << QUEEN_URL << std::endl;

        // A push names a queue and, optionally, a partition. Both are created by
        // this call if they do not exist, inside the transaction that stores the
        // message. There is no declare step and nothing to provision first.
        //
        // The broker answers with one result per message, in order. It answers
        // HTTP 201 even when it refuses one, so read each result's status.
        auto pushed = client.queue(QUEUE).push({{"greeting", "Hello World!"}});

        std::cout << "pushed " << pushed[0].transaction_id << " -> " << pushed[0].status_text
                  << std::endl;
        check(pushed[0].status == queen::PushStatus::Queued, "the broker stored the message");

        // pop() takes messages under a lease: they are claimed until they are
        // acknowledged or the lease expires. wait(true) turns on long polling, so
        // the call parks until a message arrives instead of coming back empty.
        //
        // No consumer group is named here, so the read goes through the queue's
        // own cursor, which starts at the beginning. Named groups are tutorial 2:
        // a group created after a message was pushed starts at the tail and would
        // see nothing here.
        std::vector<queen::Message> messages = client.queue(QUEUE).batch(1).wait(true).pop();

        check(messages.size() == 1, "one message came back");
        const queen::Message& message = messages[0];
        std::cout << "received \"" << message.data["greeting"].get<std::string>()
                  << "\" from partition " << message.partition << std::endl;
        check(message.data["greeting"] == "Hello World!", "the payload survived the round trip");

        // No partition was named on the push, so the broker put the message in
        // the queue's default lane.
        check(message.partition == "Default", "it landed in the default partition");

        // The acknowledgement is what commits consumption. It moves the cursor
        // past the message and releases the lease. A rejected ack still arrives
        // as HTTP 200 with success: false on the item, so ack() returns that
        // verdict as a value (a call that could not be made throws).
        queen::AckResult ack = client.ack(message);
        check(ack.success, "the acknowledgement was accepted");

        // The cursor is now past the only message, so a further read finds
        // nothing. wait(false) returns immediately instead of long polling.
        check(client.queue(QUEUE).wait(false).pop().empty(), "the queue is drained");

        // Clean up on success only: a failed run leaves the queue on the broker
        // to be looked at. del(), not delete: the word is taken.
        client.queue(QUEUE).del();

        verdict = "\nPASS: " + std::to_string(checks) + " checks";
    } catch (const std::exception& err) {
        verdict = std::string("\nFAIL: ") + err.what();
        failed = true;
    }

    // close() flushes anything still sitting in the client-side push buffers
    // (30 seconds at most) and closes the pooled connections. Nothing hangs if
    // you skip it; what you lose is the buffered pushes.
    client.close();

    (failed ? std::cerr : std::cout) << verdict << std::endl;
    return failed ? 1 : 0;
}
// docs:end
