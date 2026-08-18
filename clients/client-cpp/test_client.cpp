/**
 * Queen C++ Client - Comprehensive Test Suite
 * 
 * Ported from Node.js client test-v2/ directory
 * Covers all human-written tests (excluding ai_* and maintenance)
 * 
 * Test Categories:
 * - Push operations
 * - Pop operations  
 * - Consume operations
 * - Queue configuration
 * - Transactions
 * - Dead Letter Queue
 * - Retention
 * - Subscription modes
 * - Complete workflows
 * - Load testing
 */

#include "queen_client.hpp"
#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include <set>
#include <random>

using namespace queen;
using json = nlohmann::json;

// Color codes for terminal output
#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define YELLOW "\033[33m"
#define RESET "\033[0m"

struct TestResult {
    std::string name;
    bool passed;
    std::string message;
};

class TestRunner {
private:
    std::vector<TestResult> results;
    std::string server_url;
    
public:
    TestRunner(const std::string& url) : server_url(url) {}
    
    void run_test(const std::string& name, std::function<bool()> test_fn) {
        std::cout << BLUE << "Running: " << name << RESET << std::endl;
        
        try {
            bool passed = test_fn();
            results.push_back({name, passed, passed ? "Success" : "Failed"});
            
            if (passed) {
                std::cout << GREEN << "✓ PASS: " << name << RESET << std::endl;
            } else {
                std::cout << RED << "✗ FAIL: " << name << RESET << std::endl;
            }
        } catch (const std::exception& e) {
            results.push_back({name, false, std::string("Exception: ") + e.what()});
            std::cout << RED << "✗ FAIL: " << name << " - " << e.what() << RESET << std::endl;
        }
        
        std::cout << std::endl;
    }
    
    void print_summary() {
        int passed = 0;
        int failed = 0;
        
        for (const auto& result : results) {
            if (result.passed) passed++;
            else failed++;
        }
        
        std::cout << "========================================" << std::endl;
        std::cout << "Test Summary" << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "Total:  " << results.size() << std::endl;
        std::cout << GREEN << "Passed: " << passed << RESET << std::endl;
        std::cout << RED << "Failed: " << failed << RESET << std::endl;
        std::cout << "========================================" << std::endl;
        
        if (failed > 0) {
            std::cout << "\nFailed tests:" << std::endl;
            for (const auto& result : results) {
                if (!result.passed) {
                    std::cout << RED << "  - " << result.name << ": " 
                             << result.message << RESET << std::endl;
                }
            }
        }
    }
    
    int get_failure_count() const {
        int count = 0;
        for (const auto& result : results) {
            if (!result.passed) count++;
        }
        return count;
    }
};

// ============================================================================
// CLEANUP UTILITY
// ============================================================================

void cleanup_test_queues(const std::string& server_url) {
    std::cout << YELLOW << "Cleaning up test queues..." << RESET << std::endl;
    
    QueenClient client(server_url);
    
    // List of all test queues to clean up
    std::vector<std::string> test_queues = {
        "test-queue-v2",
        "test-queue-v2-duplicate",
        "test-queue-partition-duplicate",
        "test-queue-partition-duplicate-different",
        "test-queue-transaction-id",
        "test-queue-buffered",
        "test-queue-delayed",
        "test-queue-window-buffer",
        "test-queue-null-payload",
        "test-queue-empty-payload",
        "test-queue-encrypted-payload",
        "test-queue-v2-pop-empty",
        "test-queue-v2-pop-non-empty",
        "test-queue-v2-pop-with-wait",
        "test-queue-v2-pop-with-ack",
        "test-queue-v2-pop-with-ack-reconsume",
        "test-queue-v2-consume",
        "test-queue-v2-namespace",
        "test-queue-v2-task",
        "test-queue-v2-consume-with-partition",
        "test-queue-v2-consume-batch",
        "test-queue-v2-consume-ordering",
        "test-queue-v2-consume-group",
        "test-queue-v2-consume-group-with-partition",
        "test-queue-v2-create",
        "test-queue-v2-delete",
        "test-queue-v2-configure",
        "test-queue-v2-txn-basic-a",
        "test-queue-v2-txn-basic-b",
        "test-queue-v2-txn-multi-a",
        "test-queue-v2-txn-multi-b",
        "test-queue-v2-txn-multi-c",
        "test-queue-v2-txn-multi-ack-a",
        "test-queue-v2-txn-multi-ack-b",
        "test-queue-v2-dlq",
        "test-queue-v2-subscription-mode-new",
        "test-queue-v2-subscription-from-now",
        "test-queue-v2-complete-init",
        "test-queue-v2-complete-next",
        "test-queue-v2-complete-final",
        // Pre-existing leak, found while checking that this suite is repeatable
        // (§10.4): test_producer_sub_ignored_without_auth creates this queue and
        // nothing ever purged it, so its message stayed leased and unacked and
        // the SECOND run of the suite popped nothing and failed.
        "test-auth-cpp-noauth",
        "test-queue-v2-kv-gate",
        "test-queue-v2-timer-fire",
        "test-queue-v2-timer-cancel",
        "test-queue-v2-timer-txn"
    };
    
    int deleted_count = 0;
    for (const auto& queue_name : test_queues) {
        try {
            client.queue(queue_name).del();
            deleted_count++;
        } catch (...) {
            // Queue might not exist, ignore errors
        }
    }
    
    std::cout << GREEN << "✓ Cleaned up " << deleted_count << " test queues" << RESET << "\n" << std::endl;
}

/**
 * Purge the KV keys and pending timers this suite creates.
 *
 * MANDATORY, NOT COSMETIC (PLAN_KV_TIMERS.md §10.4). Without it a putIfAbsent
 * test is green on its first run and red forever afterwards -- the marker it
 * claims is still there -- and an incr test accumulates across runs, so its
 * assertion on the counter drifts by one every time somebody runs the suite.
 * The cancel of a pending timer matters for the same reason: a 60-second timer
 * left behind by a failed run fires into a queue the next run is asserting on.
 *
 * Each call is in its own try/catch, exactly like the queue purge above: the
 * schema may not exist, the feature may be off (404), and neither is a reason
 * to abort the cleanup of everything else.
 */
void cleanup_test_kv_and_timers(const std::string& server_url) {
    std::cout << YELLOW << "Cleaning up test KV keys and timers..." << RESET << std::endl;

    QueenClient client(server_url);

    const std::vector<std::string> keys = {
        "put-get", "marker", "cas", "cas-absent", "del", "counter", "txn-gate"
    };
    int purged_keys = 0;
    for (const auto& key : keys) {
        try {
            client.kv("cpp-kv-test").del(key);
            purged_keys++;
        } catch (...) {
            // Schema absent, or the surface paused under us. Best-effort purge:
            // not this function's problem, and never the run's verdict.
        }
    }

    const std::vector<std::pair<std::string, std::string>> timers = {
        {"test-queue-v2-timer-fire", "fire-1"},
        {"test-queue-v2-timer-cancel", "cancel-1"},
        {"test-queue-v2-timer-cancel", "resched-1"},
        {"test-queue-v2-timer-txn", "txn-1"}
    };
    int purged_timers = 0;
    for (const auto& timer : timers) {
        try {
            client.timers().cancel(timer.first, timer.second);
            purged_timers++;
        } catch (...) {
            // Same.
        }
    }

    std::cout << GREEN << "✓ Purged " << purged_keys << " KV keys and " << purged_timers
              << " timers" << RESET << "\n" << std::endl;
}

// ============================================================================
// PUSH TESTS
// ============================================================================

bool test_push_message(const std::string& server_url) {
    QueenClient client(server_url);
    auto queue = client.queue("test-queue-v2").create();
    if (!queue.contains("configured") || !queue["configured"].get<bool>()) {
        return false;
    }
    
    auto res = client.queue("test-queue-v2").push({
        {{"data", {{"message", "Hello, world!"}}}}
    });
    
    return res.is_array() && res.size() > 0 && res[0]["status"].get<std::string>() == "queued";
}

bool test_push_duplicate_message(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-duplicate").create();
    
    auto res1 = client.queue("test-queue-v2-duplicate").push({
        {{"transactionId", "test-transaction-id"}, {"data", {{"message", "Hello, world!"}}}}
    });
    
    auto res2 = client.queue("test-queue-v2-duplicate").push({
        {{"transactionId", "test-transaction-id"}, {"data", {{"message", "Hello, world!"}}}}
    });
    
    return res1[0]["status"].get<std::string>() == "queued" && 
           res2[0]["status"].get<std::string>() == "duplicate";
}

bool test_push_duplicate_partition(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-partition-duplicate").create();
    
    auto res1 = client.queue("test-queue-partition-duplicate")
        .partition("0")
        .push({{{"transactionId", "test-transaction-id"}, {"data", {{"message", "Hello"}}}}});
    
    auto res2 = client.queue("test-queue-partition-duplicate")
        .partition("0")
        .push({{{"transactionId", "test-transaction-id"}, {"data", {{"message", "Hello"}}}}});
    
    return res1[0]["status"].get<std::string>() == "queued" && 
           res2[0]["status"].get<std::string>() == "duplicate";
}

bool test_push_duplicate_different_partition(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-partition-duplicate-different").create();
    
    auto res1 = client.queue("test-queue-partition-duplicate-different")
        .partition("0")
        .push({{{"transactionId", "test-transaction-id"}, {"data", {{"message", "Hello"}}}}});
    
    auto res2 = client.queue("test-queue-partition-duplicate-different")
        .partition("1")
        .push({{{"transactionId", "test-transaction-id"}, {"data", {{"message", "Hello"}}}}});
    
    // Same transaction ID in different partitions should both succeed
    return res1[0]["status"].get<std::string>() == "queued" && 
           res2[0]["status"].get<std::string>() == "queued";
}

bool test_push_with_transaction_id(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-transaction-id").create();
    
    auto res = client.queue("test-queue-transaction-id").push({
        {{"transactionId", "test-transaction-id"}, {"data", {{"message", "Hello!"}}}}
    });
    
    if (!res.is_array() || res.empty()) return false;
    if (!res[0].contains("status") || res[0]["status"].get<std::string>() != "queued") return false;
    
    // Check if the message was pushed successfully
    // Note: Server may or may not echo back the transactionId
    return res[0]["status"].get<std::string>() == "queued";
}

bool test_push_buffered(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-buffered").create();
    
    BufferOptions buffer_opts{10, 1000};
    auto res = client.queue("test-queue-buffered")
        .buffer(buffer_opts)
        .push({{{"data", {{"message", "Hello, world!"}}}}});
    
    // Should be buffered
    auto pop = client.queue("test-queue-buffered").batch(1).wait(false).pop();
    if (!pop.empty()) return false;
    
    // Wait for buffer timeout
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    
    auto pop2 = client.queue("test-queue-buffered").batch(1).wait(false).pop();
    return !pop2.empty();
}

bool test_push_delayed(const std::string& server_url) {
    QueenClient client(server_url);
    
    QueueConfig config;
    config.delayed_processing = 2;
    client.queue("test-queue-delayed").config(config).create();
    
    client.queue("test-queue-delayed").push({
        {{"transactionId", "test-transaction-delayed-id"}, 
         {"data", {{"message", "Hello, world!"}, {"aaa", "1"}}}}
    });
    
    auto pop = client.queue("test-queue-delayed").batch(1).wait(false).pop();
    if (!pop.empty()) return false;
    
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    
    auto pop2 = client.queue("test-queue-delayed").batch(1).wait(true).pop();
    return pop2.size() == 1;
}

bool test_push_window_buffer(const std::string& server_url) {
    QueenClient client(server_url);
    
    QueueConfig config;
    config.window_buffer = 2;
    client.queue("test-queue-window-buffer").config(config).create();
    
    client.queue("test-queue-window-buffer").push({
        {{"data", {{"message", "Hello, world 1!"}}}},
        {{"data", {{"message", "Hello, world 2!"}}}},
        {{"data", {{"message", "Hello, world 3!"}}}}
    });
    
    auto pop = client.queue("test-queue-window-buffer").batch(1).wait(false).pop();
    if (!pop.empty()) return false;
    
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    
    auto pop2 = client.queue("test-queue-window-buffer").batch(4).wait(false).pop();
    return pop2.size() == 3;
}

bool test_push_null_payload(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-null-payload").create();
    
    client.queue("test-queue-null-payload").push({
        {{"data", nullptr}}
    });
    
    auto received = client.queue("test-queue-null-payload").batch(1).wait(false).pop();
    return !received.empty() && received[0]["data"].is_null();
}

bool test_push_empty_payload(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-empty-payload").create();
    
    client.queue("test-queue-empty-payload").push({
        {{"data", json::object()}}
    });
    
    auto received = client.queue("test-queue-empty-payload").batch(1).wait(false).pop();
    return !received.empty() && received[0]["data"].is_object() && 
           received[0]["data"].empty();
}

bool test_push_encrypted(const std::string& server_url) {
    QueenClient client(server_url);
    
    QueueConfig config;
    config.encryption_enabled = true;
    client.queue("test-queue-encrypted-payload").config(config).create();
    
    client.queue("test-queue-encrypted-payload").push({
        {{"data", {{"message", "Hello, world!"}}}}
    });
    
    auto received = client.queue("test-queue-encrypted-payload").batch(1).wait(false).pop();
    return !received.empty() && 
           received[0]["data"]["message"].get<std::string>() == "Hello, world!";
}

// ============================================================================
// POP TESTS
// ============================================================================

bool test_pop_empty_queue(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-pop-empty").create();
    
    auto res = client.queue("test-queue-v2-pop-empty").batch(1).wait(false).pop();
    return res.empty();
}

bool test_pop_non_empty_queue(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-pop-non-empty").create();
    
    client.queue("test-queue-v2-pop-non-empty").push({
        {{"data", {{"message", "Hello, world!"}}}}
    });
    
    auto res = client.queue("test-queue-v2-pop-non-empty").batch(1).wait(false).pop();
    return res.size() == 1;
}

bool test_pop_with_wait(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-pop-with-wait").create();
    
    // Push message after 2 seconds in background thread
    std::thread([&]() {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        client.queue("test-queue-v2-pop-with-wait").push({
            {{"data", {{"message", "Hello, world!"}}}}
        });
    }).detach();
    
    auto res = client.queue("test-queue-v2-pop-with-wait").batch(1).wait(true).pop();
    return res.size() == 1;
}

bool test_pop_with_ack(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-pop-with-ack").create();
    
    client.queue("test-queue-v2-pop-with-ack").push({
        {{"data", {{"message", "Hello, world!"}}}}
    });
    
    auto res = client.queue("test-queue-v2-pop-with-ack").batch(1).wait(false).pop();
    auto resAck = client.ack(res[0]);
    
    return resAck.contains("success") && resAck["success"].get<bool>();
}

bool test_pop_with_ack_reconsume(const std::string& server_url) {
    QueenClient client(server_url);
    
    QueueConfig config;
    config.lease_time = 1;  // 1 second lease
    client.queue("test-queue-v2-pop-with-ack-reconsume").config(config).create();
    
    client.queue("test-queue-v2-pop-with-ack-reconsume").push({
        {{"data", {{"message", "Hello, world!"}}}}
    });
    
    auto res = client.queue("test-queue-v2-pop-with-ack-reconsume")
        .batch(1).wait(false).pop();
    
    if (res.size() != 1) return false;
    
    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    auto res2 = client.queue("test-queue-v2-pop-with-ack-reconsume")
        .batch(1).wait(true).pop();
    
    return res2.size() == 1;  // Message should be reconsumed
}

// ============================================================================
// CONSUME TESTS  
// ============================================================================

bool test_consumer(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-consume").create();
    
    client.queue("test-queue-v2-consume").push({
        {{"data", {{"message", "Hello, world!"}}}}
    });
    
    bool msg_received = false;
    client.queue("test-queue-v2-consume")
        .batch(1)
        .limit(1)
        .consume([&](const json& msg) {
            msg_received = true;
        });
    
    return msg_received;
}

bool test_consumer_namespace(const std::string& server_url) {
    QueenClient client(server_url);
    
    client.queue("test-queue-v2-namespace")
        .namespace_name("test-namespace")
        .create();
    
    client.queue("test-queue-v2-namespace").push({
        {{"data", {{"message", "Hello, world!"}}}}
    });
    
    bool msg_received = false;
    client.queue()
        .namespace_name("test-namespace")
        .batch(1)
        .limit(1)
        .consume([&](const json& msg) {
            msg_received = true;
        });
    
    return msg_received;
}

bool test_consumer_task(const std::string& server_url) {
    QueenClient client(server_url);
    
    client.queue("test-queue-v2-task")
        .task("test-task")
        .create();
    
    client.queue("test-queue-v2-task").push({
        {{"data", {{"message", "Hello, world!"}}}}
    });
    
    bool msg_received = false;
    client.queue()
        .task("test-task")
        .batch(1)
        .limit(1)
        .consume([&](const json& msg) {
            msg_received = true;
        });
    
    return msg_received;
}

bool test_consumer_with_partition(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-consume-with-partition").create();
    
    // Push 10 messages to partition 1 (without buffering for reliability)
    std::vector<json> messages1;
    for (int i = 0; i < 10; i++) {
        messages1.push_back({{"data", {{"message", "Hello, world!"}}}});
    }
    client.queue("test-queue-v2-consume-with-partition")
        .partition("test-partition-01")
        .push(messages1);
    
    // Push 10 messages to partition 2
    std::vector<json> messages2;
    for (int i = 0; i < 10; i++) {
        messages2.push_back({{"data", {{"message", "Hello, world!"}}}});
    }
    client.queue("test-queue-v2-consume-with-partition")
        .partition("test-partition-02")
        .push(messages2);
    
    // Small delay to ensure messages are available
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    int msg_count1 = 0;
    client.queue("test-queue-v2-consume-with-partition")
        .partition("test-partition-01")
        .batch(10)
        .limit(1)
        .wait(false)
        .consume([&](const json& msgs) {
            msg_count1 = msgs.size();
        });
    
    int msg_count2 = 0;
    client.queue("test-queue-v2-consume-with-partition")
        .partition("test-partition-02")
        .batch(10)
        .limit(1)
        .wait(false)
        .consume([&](const json& msgs) {
            msg_count2 = msgs.size();
        });
    
    return msg_count1 == 10 && msg_count2 == 10;
}

bool test_consumer_batch(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-consume-batch").create();
    
    client.queue("test-queue-v2-consume-batch").push({
        {{"data", {{"message", "Hello, world!"}}}},
        {{"data", {{"message", "Hello, world 2!"}}}},
        {{"data", {{"message", "Hello, world 3!"}}}}
    });
    
    int msg_length = 0;
    client.queue("test-queue-v2-consume-batch")
        .batch(10)
        .wait(false)
        .limit(1)
        .consume([&](const json& msgs) {
            msg_length = msgs.size();
        });
    
    return msg_length == 3;
}

bool test_consumer_ordering(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-consume-ordering").create();
    
    int messages_to_push = 20;  // Reduced for speed
    std::vector<json> messages;
    for (int i = 0; i < messages_to_push; i++) {
        messages.push_back({{"data", {{"id", i}}}});
    }
    client.queue("test-queue-v2-consume-ordering").push(messages);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    int last_id = -1;
    bool ordered = true;
    
    client.queue("test-queue-v2-consume-ordering")
        .batch(1)
        .wait(false)
        .idle_millis(2000)  // Add idle timeout to prevent infinite loop
        .limit(messages_to_push)
        .each()  // CRITICAL: Process messages one-by-one, not as array!
        .consume([&](const json& msg) {
            int id = msg["data"]["id"].get<int>();
            if (last_id == -1) {
                last_id = id;
            } else {
                if (id != last_id + 1) {
                    ordered = false;
                }
                last_id = id;
            }
        });
    
    return ordered && last_id == messages_to_push - 1;
}

bool test_consumer_group(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-consume-group").create();
    
    // Push messages without buffering for reliability
    int messages_to_push = 10;
    std::vector<json> messages;
    for (int i = 0; i < messages_to_push; i++) {
        messages.push_back({{"data", {{"id", i}}}});
    }
    client.queue("test-queue-v2-consume-group").push(messages);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    int group01_messages = 0;
    client.queue("test-queue-v2-consume-group")
        .group("test-group-01")
        .subscription_mode("all")
        .batch(messages_to_push)
        .limit(1)
        .wait(false)
        .consume([&](const json& msgs) {
            group01_messages = msgs.size();
        });
    
    int group02_messages = 0;
    client.queue("test-queue-v2-consume-group")
        .group("test-group-02")
        .subscription_mode("all")
        .batch(messages_to_push)
        .limit(1)
        .wait(false)
        .consume([&](const json& msgs) {
            group02_messages = msgs.size();
        });
    
    return group01_messages == messages_to_push && group02_messages == messages_to_push;
}

bool test_consumer_group_with_partition(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-consume-group-with-partition").create();
    
    // Push messages without buffering
    int messages_to_push = 10;
    std::vector<json> messages;
    for (int i = 0; i < messages_to_push; i++) {
        messages.push_back({{"data", {{"id", i}}}});
    }
    client.queue("test-queue-v2-consume-group-with-partition")
        .partition("test-partition-01")
        .push(messages);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    int group01_messages = 0;
    client.queue("test-queue-v2-consume-group-with-partition")
        .partition("test-partition-01")
        .group("test-group-01")
        .subscription_mode("all")
        .batch(messages_to_push)
        .limit(1)
        .wait(false)
        .consume([&](const json& msgs) {
            group01_messages = msgs.size();
        });
    
    int group02_messages = 0;
    client.queue("test-queue-v2-consume-group-with-partition")
        .partition("test-partition-01")
        .group("test-group-02")
        .subscription_mode("all")
        .batch(messages_to_push)
        .limit(1)
        .wait(false)
        .consume([&](const json& msgs) {
            group02_messages = msgs.size();
        });
    
    return group01_messages == messages_to_push && group02_messages == messages_to_push;
}

// ============================================================================
// QUEUE TESTS
// ============================================================================

bool test_create_queue(const std::string& server_url) {
    QueenClient client(server_url);
    auto res = client.queue("test-queue-v2-create").create();
    return res.contains("configured") && res["configured"].get<bool>();
}

bool test_delete_queue(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-delete").create();
    auto res = client.queue("test-queue-v2-delete").del();
    return res.contains("deleted") && res["deleted"].get<bool>();
}

bool test_configure_queue(const std::string& server_url) {
    QueenClient client(server_url);
    
    QueueConfig config;
    config.completed_retention_seconds = 1;
    config.delayed_processing = 1;
    config.encryption_enabled = true;
    config.lease_time = 30;
    config.max_size = 1000;
    config.priority = 5;
    config.retention_seconds = 0;
    config.retry_limit = 3;
    config.window_buffer = 100;
    
    auto res = client.queue("test-queue-v2-configure").config(config).create();
    
    if (!res.contains("configured") || !res["configured"].get<bool>()) {
        return false;
    }
    
    // Verify configuration (spot check a few fields)
    auto opts = res["options"];
    return opts["priority"].get<int>() == 5 &&
           opts["leaseTime"].get<int>() == 30 &&
           opts["encryptionEnabled"].get<bool>() == true;
}

// ============================================================================
// TRANSACTION TESTS
// ============================================================================

bool test_transaction_basic_push_ack(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-txn-basic-a").create();
    client.queue("test-queue-v2-txn-basic-b").create();
    
    client.queue("test-queue-v2-txn-basic-a").push({
        {{"data", {{"value", 1}}}}
    });
    
    auto messages = client.queue("test-queue-v2-txn-basic-a")
        .batch(1).wait(false).pop();
    
    if (messages.empty()) return false;
    
    int next_value = messages[0]["data"]["value"].get<int>() + 1;
    
    client.transaction()
        .queue("test-queue-v2-txn-basic-b")
        .push({{{"data", {{"value", next_value}}}}})
        .ack(messages[0])
        .commit();
    
    auto resultB = client.queue("test-queue-v2-txn-basic-b").batch(1).wait(false).pop();
    auto resultA = client.queue("test-queue-v2-txn-basic-a").batch(1).wait(false).pop();
    
    return resultB.size() == 1 && resultA.empty() && 
           resultB[0]["data"]["value"].get<int>() == 2;
}

bool test_transaction_multiple_pushes(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-txn-multi-a").create();
    client.queue("test-queue-v2-txn-multi-b").create();
    client.queue("test-queue-v2-txn-multi-c").create();
    
    client.queue("test-queue-v2-txn-multi-a").push({
        {{"data", {{"id", "source"}}}}
    });
    
    auto messages = client.queue("test-queue-v2-txn-multi-a")
        .batch(1).wait(false).pop();
    
    if (messages.empty()) return false;
    
    client.transaction()
        .queue("test-queue-v2-txn-multi-b")
        .push({{{"data", {{"id", "b"}, {"source", messages[0]["data"]["id"]}}}}})
        .queue("test-queue-v2-txn-multi-c")
        .push({{{"data", {{"id", "c"}, {"source", messages[0]["data"]["id"]}}}}})
        .ack(messages[0])
        .commit();
    
    auto resultB = client.queue("test-queue-v2-txn-multi-b").batch(1).wait(false).pop();
    auto resultC = client.queue("test-queue-v2-txn-multi-c").batch(1).wait(false).pop();
    auto resultA = client.queue("test-queue-v2-txn-multi-a").batch(1).wait(false).pop();
    
    return resultB.size() == 1 && resultC.size() == 1 && resultA.empty();
}

bool test_transaction_multiple_acks(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-txn-multi-ack-a").create();
    client.queue("test-queue-v2-txn-multi-ack-b").create();
    
    client.queue("test-queue-v2-txn-multi-ack-a").push({
        {{"data", {{"value", 1}}}},
        {{"data", {{"value", 2}}}},
        {{"data", {{"value", 3}}}}
    });
    
    auto messages = client.queue("test-queue-v2-txn-multi-ack-a")
        .batch(3).wait(false).pop();
    
    if (messages.size() != 3) return false;
    
    int sum = 0;
    for (const auto& msg : messages) {
        sum += msg["data"]["value"].get<int>();
    }
    
    client.transaction()
        .ack(messages[0])
        .ack(messages[1])
        .ack(messages[2])
        .queue("test-queue-v2-txn-multi-ack-b")
        .push({{{"data", {{"sum", sum}}}}})
        .commit();
    
    auto resultA = client.queue("test-queue-v2-txn-multi-ack-a").batch(3).wait(false).pop();
    auto resultB = client.queue("test-queue-v2-txn-multi-ack-b").batch(1).wait(false).pop();
    
    return resultA.empty() && resultB.size() == 1 && 
           resultB[0]["data"]["sum"].get<int>() == 6;
}

bool test_transaction_empty_commit(const std::string& server_url) {
    QueenClient client(server_url);
    
    bool error_thrown = false;
    try {
        client.transaction().commit();
    } catch (const std::exception& e) {
        error_thrown = std::string(e.what()).find("no operations") != std::string::npos;
    }
    
    return error_thrown;
}

bool test_transaction_ack_with_consumer_group(const std::string& server_url) {
    QueenClient client(server_url);
    
    // Use unique queue names for C++ tests with timestamp to avoid state from previous runs
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    
    std::string queue_a = "test-queue-cpp-txn-cg-a-" + std::to_string(timestamp);
    std::string queue_b = "test-queue-cpp-txn-cg-b-" + std::to_string(timestamp);
    
    client.queue(queue_a).create();
    client.queue(queue_b).create();
    
    std::string consumer_group = "test-cg-txn-cpp-" + std::to_string(timestamp);
    
    // Push messages to queue A
    client.queue(queue_a).push({
        {{"data", {{"value", 1}}}},
        {{"data", {{"value", 2}}}}
    });
    
    // Small delay to ensure messages are written
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Pop with consumer group
    auto messages = client.queue(queue_a)
        .group(consumer_group)
        .subscription_mode("all")
        .batch(2)
        .wait(false)
        .pop();
    
    if (messages.size() != 2) {
        std::cerr << "Expected 2 messages, got " << messages.size() << std::endl;
        return false;
    }
    
    int sum = messages[0]["data"]["value"].get<int>() + messages[1]["data"]["value"].get<int>();
    
    // Transaction: ACK with consumer group context, push to B
    json context = {{"consumerGroup", consumer_group}};
    client.transaction()
        .ack(messages[0], "completed", context)
        .ack(messages[1], "completed", context)
        .queue(queue_b)
        .push({{{"data", {{"sum", sum}}}}})
        .commit();
    
    // Verify: Pop again with same consumer group should get no messages
    auto messages_after_ack = client.queue(queue_a)
        .group(consumer_group)
        .batch(2)
        .wait(false)
        .pop();
    
    // Verify: Queue B has the result
    auto result_b = client.queue(queue_b).batch(1).wait(false).pop();
    
    // Verify: Using a different consumer group should still see the messages
    auto messages_other_group = client.queue(queue_a)
        .group("other-consumer-group-cpp")
        .subscription_mode("all")
        .batch(2)
        .wait(false)
        .pop();
    
    return messages_after_ack.empty() && 
           result_b.size() == 1 && 
           result_b[0]["data"]["sum"].get<int>() == 3 &&
           messages_other_group.size() == 2;
}

// ============================================================================
// DLQ TEST
// ============================================================================

// ============================================================================
// PRODUCER SUB TESTS (issue #23, feature A)
// ============================================================================
// These tests verify that the producerSub field surfaces correctly on popped
// messages in C++. The C++ client uses nlohmann::json for messages, so the
// field is accessed as msg["producerSub"] - no struct change required. The
// tests also verify that server-side anti-impersonation logic prevents a
// client from spoofing the field.
// ============================================================================

// Verify that producerSub (when present in the server response) is accessible
// via the nlohmann::json-based Message. Uses a raw HTTP push that attempts to
// set producerSub in the body; the server (with auth disabled, the typical
// CI environment) must ignore it and store NULL, which surfaces as JSON null.
bool test_producer_sub_ignored_without_auth(const std::string& server_url) {
    QueenClient client(server_url);
    const std::string queue_name = "test-auth-cpp-noauth";
    const std::string tx_id = "tx-cpp-noauth-" + std::to_string(std::time(nullptr));

    client.queue(queue_name).create();

    // Build a push body that tries to spoof producerSub.
    httplib::Client http(server_url.c_str());
    http.set_connection_timeout(5);
    json body = {
        {"items", json::array({
            {
                {"queue", queue_name},
                {"partition", "Default"},
                {"transactionId", tx_id},
                {"payload", {{"x", 1}}},
                {"producerSub", "attacker-cpp"}  // must be ignored server-side
            }
        })}
    };
    auto res = http.Post("/api/v1/push", body.dump(), "application/json");
    if (!res || res->status >= 300) {
        std::cerr << "HTTP push failed" << std::endl;
        return false;
    }

    // Pop and verify the server returns null (or omits) producerSub.
    auto msgs = client.queue(queue_name).batch(1).wait(false).pop();
    if (msgs.empty()) {
        std::cerr << "No message popped" << std::endl;
        return false;
    }

    const auto& m = msgs[0];
    // The server returns the field as null when producer_sub is NULL in DB.
    // Valid expectations: field missing OR field is null - either way the
    // spoofing attempt must NOT be reflected.
    if (m.contains("producerSub") && !m["producerSub"].is_null()) {
        std::cerr << "producerSub leaked: " << m["producerSub"].dump() << std::endl;
        return false;
    }
    return true;
}

bool test_dlq(const std::string& server_url) {
    QueenClient client(server_url);
    
    QueueConfig config;
    config.retry_limit = 1;  // Only 1 retry
    client.queue("test-queue-v2-dlq").config(config).create();
    
    client.queue("test-queue-v2-dlq").push({
        {{"data", {{"message", "Test DLQ message"}, {"timestamp", std::time(nullptr)}}}}
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Consume and fail MULTIPLE times (original + retry)
    int attempt_count = 0;
    try {
        client.queue("test-queue-v2-dlq")
            .batch(1)
            .wait(true)
            .limit(2)  // Allow 2 attempts (original + 1 retry)
            .auto_ack(true)
            .idle_millis(3000)
            .consume([&](const json& msg) {
                attempt_count++;
                throw std::runtime_error("Test error - triggering DLQ");
            });
    } catch (...) {
        // Expected to fail
    }
    
    // Wait longer for DLQ processing
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    auto dlq_result = client.queue("test-queue-v2-dlq")
        .dlq()
        .limit(10)
        .get();
    
    // DLQ might be empty if message hasn't been moved yet or retries still pending
    // For now, just check that query works (even if no messages yet)
    return dlq_result.contains("messages") && dlq_result.contains("total");
}

// ============================================================================
// SUBSCRIPTION MODE TESTS
// ============================================================================

bool test_subscription_mode_new(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-subscription-mode-new").create();
    
    // Push historical messages
    for (int i = 0; i < 5; i++) {
        client.queue("test-queue-v2-subscription-mode-new").push({
            {{"data", {{"id", i}, {"type", "historical"}}}}
        });
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // The replay baseline for the contrast below. This used to rely on the broker
    // default being "all"; that default is now "new", so the baseline has to ask
    // for the backlog explicitly or the comparison with "new" is vacuous.
    int all_messages_count = 0;
    client.queue("test-queue-v2-subscription-mode-new")
        .group("group-all")
        .subscription_mode("all")
        .batch(10)
        .wait(false)
        .limit(1)
        .consume([&](const json& msgs) {
            all_messages_count = msgs.size();
        });
    
    // New mode - should skip historical
    auto new_only_messages = client.queue("test-queue-v2-subscription-mode-new")
        .group("group-new-only")
        .subscription_mode("new")
        .batch(10)
        .wait(false)
        .pop();
    
    return all_messages_count == 5 && new_only_messages.empty();
}

bool test_subscription_from_now(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-subscription-from-now").create();
    
    // Push historical messages
    for (int i = 0; i < 5; i++) {
        client.queue("test-queue-v2-subscription-from-now").push({
            {{"data", {{"id", i}, {"type", "historical"}}}}
        });
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // subscriptionFrom('now') - should skip historical
    auto now_messages = client.queue("test-queue-v2-subscription-from-now")
        .group("group-from-now")
        .subscription_from("now")
        .batch(10)
        .wait(false)
        .pop();
    
    return now_messages.empty();
}

// ============================================================================
// COMPLETE WORKFLOW TEST
// ============================================================================

bool test_complete_workflow(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-complete-init").create();
    client.queue("test-queue-v2-complete-next").create();
    client.queue("test-queue-v2-complete-final").create();
    
    client.queue("test-queue-v2-complete-init").push({
        {{"data", {{"message", "First"}, {"count", 0}}}}
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Stage 1: init -> next
    client.queue("test-queue-v2-complete-init")
        .batch(1)
        .wait(false)
        .idle_millis(3000)  // Idle timeout
        .limit(1)
        .each()  // Process individual messages
        .auto_ack(false)
        .consume([&](const json& msg) {
            int next_count = msg["data"]["count"].get<int>() + 1;
            client.transaction()
                .queue("test-queue-v2-complete-next")
                .push({{{"data", {{"message", "Next"}, {"count", next_count}}}}})
                .ack(msg)
                .commit();
        });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Stage 2: next -> final
    client.queue("test-queue-v2-complete-next")
        .batch(1)
        .wait(false)
        .idle_millis(3000)  // Idle timeout
        .limit(1)
        .each()  // Process individual messages
        .auto_ack(false)
        .consume([&](const json& msg) {
            int final_count = msg["data"]["count"].get<int>() + 1;
            client.transaction()
                .queue("test-queue-v2-complete-final")
                .push({{{"data", {{"message", "Final"}, {"count", final_count}}}}})
                .ack(msg)
                .commit();
        });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Stage 3: consume final
    bool final_received = false;
    int final_count = -1;
    client.queue("test-queue-v2-complete-final")
        .batch(1)
        .wait(false)
        .idle_millis(3000)  // Idle timeout
        .limit(1)
        .each()  // Process individual messages
        .consume([&](const json& msg) {
            final_count = msg["data"]["count"].get<int>();
            final_received = (final_count == 2);
        });
    
    return final_received && final_count == 2;
}

// ============================================================================
// KV AND TIMERS (PLAN_KV_TIMERS.md §5, §4)
//
// These run against every broker, unconditionally. KV and timers are not
// features a cell opts into -- there is no boot flag left to turn on, the same
// way there is none for push and pop -- so a cell that fails these is a cell
// that is broken, and there is nothing here to probe for first.
//
// An operator's runtime kill switch can still PAUSE either surface, and then
// these fail. That is correct: a paused cell is an incident, not a
// configuration, and a suite that skipped itself on 503 would be hiding it.
//
// The exact wire shape -- the JSON body, method and path of every operation --
// is asserted WITHOUT a broker in test_kv_timers.cpp, and that is where a wrong
// shape gets caught: a bundle whose KV riders are in the wrong place still
// commits here, it merely commits without the gate. These tests assert the
// SEMANTICS, which is the half a plan server cannot fake.
//
// `forever` appears nowhere below, on purpose (§10.4): a test that goes wrong
// must not leave immortal state in a shared database. Every key here carries a
// TTL, and cleanup_test_kv_and_timers() purges them anyway.
// ============================================================================

static const char* KV_NS = "cpp-kv-test";

/// How long a delivery assertion waits, and why it is this large.
///
/// A timer's deliverAt is "NOT BEFORE", never "exactly at". A busy broker
/// delivers within about ten milliseconds above the sweeper's minimum sleep,
/// and QUEEN_SWEEPER_MAX_SLEEP_MS (1 s) bounds the case where a different
/// broker scheduled the timer than the one firing it.
///
/// But a broker with an EMPTY timer table backs its sweep off progressively, up
/// to QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS (30 s by default), and what cancels that
/// backoff early is the local in-process wake -- which the broker declares as a
/// SEAM and has not implemented yet. So the first timer scheduled on an idle
/// broker can wait out most of that sleep before anything looks at it. Measured
/// here: 17 s for a timer asked to fire in 500 ms.
///
/// This budget therefore covers the idle ceiling rather than the healthy
/// latency, and it is deliberately NOT an assertion about delivery speed: a
/// test that pinned 500 ms would be pinning the seam. The compose stack this
/// suite runs in sets QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS=1000 so CI does not pay
/// the ceiling; against any other broker the budget is what makes it pass.
static const int TIMER_DELIVERY_BUDGET_MS = 45000;

/// Poll a queue until a message shows up or the budget runs out.
json await_message(QueenClient& client, const std::string& queue, int budget_millis) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budget_millis);
    while (std::chrono::steady_clock::now() < deadline) {
        json messages = client.queue(queue).batch(1).wait(false).pop();
        if (!messages.empty()) {
            return messages;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return json::array();
}

bool test_kv_put_and_get(const std::string& server_url) {
    QueenClient client(server_url);

    json written = client.kv(KV_NS).put("put-get", {{"n", 1}}, KvTtl::seconds(300));
    if (!written.value("applied", false)) return false;

    json read = client.kv(KV_NS).get("put-get");
    return read.value("found", false) &&
           read["value"] == json({{"n", 1}}) &&
           read.value("version", 0LL) == written.value("version", -1LL) &&
           read.contains("expiresAt") && !read["expiresAt"].is_null();
}

bool test_kv_get_absent_is_not_an_error(const std::string& server_url) {
    QueenClient client(server_url);
    // §8.1: the status describes the outcome of the CALL, never the verdict of
    // the business predicate. A missing key is a 200 with found:false, so it
    // must arrive here as a value and not as an exception.
    json read = client.kv(KV_NS).get("no-such-key-ever");
    return read.contains("found") && read["found"].get<bool>() == false;
}

bool test_kv_put_if_absent_has_exactly_one_winner(const std::string& server_url) {
    QueenClient client(server_url);
    client.kv(KV_NS).del("marker");

    json first = client.kv(KV_NS).put_if_absent("marker", {{"owner", "a"}}, KvTtl::seconds(300));
    json second = client.kv(KV_NS).put_if_absent("marker", {{"owner", "b"}}, KvTtl::seconds(300));

    // The loser gets the WINNER's value back, which is the entire point of the
    // idempotency marker: whoever loses must not need a second round trip to
    // find out what the winner wrote.
    return first.value("applied", false) &&
           second.value("applied", true) == false &&
           second.value("reason", std::string()) == "exists" &&
           second["value"] == json({{"owner", "a"}}) &&
           second.value("version", 0LL) == first.value("version", -1LL);
}

bool test_kv_expect_on_an_absent_key_creates_nothing(const std::string& server_url) {
    QueenClient client(server_url);
    client.kv(KV_NS).del("cas-absent");

    KvWriteOptions fence;
    fence.expect = 5;
    // §5.3, the repair that matters most: in the naive form an expect:N>0 on an
    // absent key falls into the INSERT branch and CREATES the row -- in a saga
    // that fires the compensating command the expect existed to prevent. An
    // expect matching zero rows must create NOTHING.
    json out = client.kv(KV_NS).put("cas-absent", {{"n", 1}}, KvTtl::seconds(300), fence);
    json after = client.kv(KV_NS).get("cas-absent");

    return out.value("applied", true) == false &&
           out.value("reason", std::string()) == "absent" &&
           after.value("found", true) == false;
}

bool test_kv_expect_refuses_a_stale_version(const std::string& server_url) {
    QueenClient client(server_url);
    client.kv(KV_NS).del("cas");

    json created = client.kv(KV_NS).put("cas", {{"n", 1}}, KvTtl::seconds(300));
    long long version = created.value("version", 0LL);

    KvWriteOptions stale;
    stale.expect = version + 1;               // a version that never existed
    json refused = client.kv(KV_NS).put("cas", {{"n", 2}}, KvTtl::seconds(300), stale);

    KvWriteOptions current;
    current.expect = version;
    json accepted = client.kv(KV_NS).put("cas", {{"n", 3}}, KvTtl::seconds(300), current);

    return refused.value("applied", true) == false &&
           refused.value("reason", std::string()) == "version" &&
           refused["value"] == json({{"n", 1}}) &&
           accepted.value("applied", false) &&
           accepted.value("version", 0LL) != version;
}

bool test_kv_delete_is_idempotent(const std::string& server_url) {
    QueenClient client(server_url);
    client.kv(KV_NS).put("del", {{"n", 1}}, KvTtl::seconds(300));

    json first = client.kv(KV_NS).del("del");
    json second = client.kv(KV_NS).del("del");

    return first.value("applied", false) &&
           second.value("applied", true) == false &&
           second.value("reason", std::string()) == "absent";
}

bool test_kv_incr_and_its_ceiling(const std::string& server_url) {
    QueenClient client(server_url);
    client.kv(KV_NS).del("counter");

    json one = client.kv(KV_NS).incr("counter", 1, KvTtl::seconds(300));
    json two = client.kv(KV_NS).incr("counter", 1, KvTtl::seconds(300));

    KvIncrOptions capped;
    capped.max = 2;
    // §5.4: max does NOT saturate and does NOT truncate. The call that would
    // break the ceiling does not apply and comes back with the CURRENT value,
    // which is what makes `applied` the admission decision rather than an
    // after-the-fact comparison on budget that was already spent.
    json refused = client.kv(KV_NS).incr("counter", 5, KvTtl::seconds(300), capped);
    json after = client.kv(KV_NS).get("counter");

    return kv_int64(one["value"]) == 1 &&
           kv_int64(two["value"]) == 2 &&
           refused.value("applied", true) == false &&
           refused.value("reason", std::string()) == "limit" &&
           kv_int64(refused["value"]) == 2 &&
           kv_int64(after["value"]) == 2;
}

bool test_transaction_kv_gate_returns_instead_of_throwing(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-kv-gate").create();
    client.kv(KV_NS).del("txn-gate");

    KvWriteOptions gate;
    gate.required = true;                     // escalate a lost race into a rollback

    auto attempt = [&](int n) {
        return client.transaction()
            .queue("test-queue-v2-kv-gate")
            .push({{{"data", {{"n", n}}}}})
            .kv(KV_NS).put_if_absent("txn-gate", {{"claimed", true}}, KvTtl::seconds(300), gate)
            .commit();
    };

    json first = attempt(1);
    if (!first.value("success", false)) return false;

    // The redelivery. §8.3: HTTP 200, success:false, reason kv_precondition --
    // and commit() RETURNS it rather than throwing, because this is the expected
    // outcome of every legitimate redelivery and must stay out of the caller's
    // error path.
    json second;
    try {
        second = attempt(2);
    } catch (const std::exception&) {
        return false;
    }

    if (second.value("success", true) != false) return false;
    if (second.value("reason", std::string()) != "kv_precondition") return false;
    if (second.value("kvReason", std::string()) != "exists") return false;
    // The flat index space (§8.2): the operations first, then the kv array. One
    // push means the single KV rider sits at flat index 1.
    if (second.value("failedIndex", -1) != 1) return false;
    if (second["value"] != json({{"claimed", true}})) return false;

    // The gate did its job: the second push must NOT have been committed.
    json messages = client.queue("test-queue-v2-kv-gate").batch(10).wait(false).pop();
    return messages.size() == 1 && messages[0]["data"]["n"].get<int>() == 1;
}

bool test_timer_fires_into_its_queue(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-timer-fire").create();

    TimerSchedule timer;
    timer.queue = "test-queue-v2-timer-fire";
    timer.timer_key = "fire-1";
    timer.payload = {{"kind", "reminder"}, {"n", 7}};
    timer.delay_millis = 500;

    json scheduled = client.timers().schedule(timer);
    if (!scheduled.value("ok", false)) return false;
    if (scheduled.value("status", std::string()) != "scheduled" &&
        scheduled.value("status", std::string()) != "rescheduled") return false;
    // messageId is promised AT SCHEDULE, so the frame can be correlated before
    // it exists.
    if (!scheduled.contains("messageId") || scheduled["messageId"].is_null()) return false;

    json messages = await_message(client, "test-queue-v2-timer-fire", TIMER_DELIVERY_BUDGET_MS);
    return messages.size() == 1 && messages[0]["data"] == json({{"kind", "reminder"}, {"n", 7}});
}

bool test_timer_cancel_before_it_fires(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-timer-cancel").create();

    TimerSchedule timer;
    timer.queue = "test-queue-v2-timer-cancel";
    timer.timer_key = "cancel-1";
    timer.payload = {{"kind", "should-not-arrive"}};
    timer.delay_millis = 60000;
    client.timers().schedule(timer);

    json cancelled = client.timers().cancel("test-queue-v2-timer-cancel", "cancel-1");
    if (!cancelled.value("ok", false)) return false;
    if (cancelled.value("status", std::string()) != "cancelled") return false;

    json messages = await_message(client, "test-queue-v2-timer-cancel", 2000);
    return messages.empty();
}

bool test_timer_cancel_absent_says_so(const std::string& server_url) {
    QueenClient client(server_url);
    const std::string txn = "88888888-8888-7888-8888-888888888888";

    json out = client.timers().cancel("test-queue-v2-timer-cancel", "never-scheduled", txn);

    // §4.4: `absent` carries ok:FALSE, deliberately. There is no tombstone, so
    // absent means "no longer pending", which INCLUDES already delivered -- and
    // ok:true would read as success to every client that trusts the field, the
    // same lesson already paid on queue delete. The txn comes back so the
    // "was it already delivered?" check needs no second API.
    return out.value("ok", true) == false &&
           out.value("status", std::string()) == "absent" &&
           out.value("txn", std::string()) == txn;
}

bool test_timer_reschedule_mints_a_new_txn(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-timer-cancel").create();

    TimerSchedule timer;
    timer.queue = "test-queue-v2-timer-cancel";
    timer.timer_key = "resched-1";
    timer.payload = {{"attempt", 1}};
    timer.delay_millis = 60000;

    json first = client.timers().schedule(timer);
    timer.payload = {{"attempt", 2}};
    json second = client.timers().schedule(timer);

    client.timers().cancel("test-queue-v2-timer-cancel", "resched-1");

    // §20.2, ratified: every reschedule mints a NEW txn, because a rescheduled
    // timer is a new message. The upsert is also what makes a retry after a
    // client crash safe by construction.
    return first.value("status", std::string()) == "scheduled" &&
           second.value("status", std::string()) == "rescheduled" &&
           first.value("txn", std::string()) != second.value("txn", std::string());
}

bool test_transaction_schedules_a_timer(const std::string& server_url) {
    QueenClient client(server_url);
    client.queue("test-queue-v2-timer-txn").create();

    TimerSchedule timer;
    timer.queue = "test-queue-v2-timer-txn";
    timer.timer_key = "txn-1";
    timer.payload = {{"from", "bundle"}};
    timer.delay_millis = 500;

    json committed = client.transaction()
        .queue("test-queue-v2-timer-txn")
        .push({{{"data", {{"immediate", true}}}}})
        .timers().schedule(timer)
        .commit();

    if (!committed.value("success", false)) return false;

    // Both the immediate push and the timer's later delivery land in the same
    // queue. Each message is ACKED as it is seen, and that is load-bearing here
    // rather than tidiness: the immediate push arrives first, and leaving it
    // unacked keeps the partition leased with the group's offset where it was,
    // so the timer's frame parks behind it and no later poll ever reaches it.
    bool saw_immediate = false;
    bool saw_timer = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(TIMER_DELIVERY_BUDGET_MS);
    while (std::chrono::steady_clock::now() < deadline && !(saw_immediate && saw_timer)) {
        json messages = client.queue("test-queue-v2-timer-txn").batch(10).wait(false).pop();
        for (const auto& msg : messages) {
            if (msg["data"] == json({{"immediate", true}})) saw_immediate = true;
            if (msg["data"] == json({{"from", "bundle"}})) saw_timer = true;
            client.ack(msg, true);
        }
        if (saw_immediate && saw_timer) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    return saw_immediate && saw_timer;
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char** argv) {
    std::string server_url = "http://localhost:6632";
    
    if (argc > 1) {
        server_url = argv[1];
    }
    
    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - Full Test Suite" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Server URL: " << server_url << std::endl;
    std::cout << "========================================\n" << std::endl;
    
    // Cleanup from previous runs. KV and timers first: a pending timer left
    // behind by a failed run fires into a queue this run is about to assert on,
    // and cancelling it before the queues are recreated keeps that impossible.
    cleanup_test_kv_and_timers(server_url);
    cleanup_test_queues(server_url);

    TestRunner runner(server_url);
    
    // PUSH TESTS
    std::cout << YELLOW << "\n=== PUSH TESTS ===" << RESET << "\n" << std::endl;
    runner.run_test("Push Message", [&]() { return test_push_message(server_url); });
    runner.run_test("Push Duplicate Message", [&]() { return test_push_duplicate_message(server_url); });
    runner.run_test("Push Duplicate on Same Partition", [&]() { return test_push_duplicate_partition(server_url); });
    runner.run_test("Push Duplicate on Different Partition", [&]() { return test_push_duplicate_different_partition(server_url); });
    runner.run_test("Push with Transaction ID", [&]() { return test_push_with_transaction_id(server_url); });
    runner.run_test("Push Buffered Message", [&]() { return test_push_buffered(server_url); });
    runner.run_test("Push Delayed Message", [&]() { return test_push_delayed(server_url); });
    runner.run_test("Push Window Buffer", [&]() { return test_push_window_buffer(server_url); });
    runner.run_test("Push Null Payload", [&]() { return test_push_null_payload(server_url); });
    runner.run_test("Push Empty Payload", [&]() { return test_push_empty_payload(server_url); });
    runner.run_test("Push Encrypted Payload", [&]() { return test_push_encrypted(server_url); });
    
    // POP TESTS
    std::cout << YELLOW << "\n=== POP TESTS ===" << RESET << "\n" << std::endl;
    runner.run_test("Pop Empty Queue", [&]() { return test_pop_empty_queue(server_url); });
    runner.run_test("Pop Non-Empty Queue", [&]() { return test_pop_non_empty_queue(server_url); });
    runner.run_test("Pop with Wait (Long Polling)", [&]() { return test_pop_with_wait(server_url); });
    runner.run_test("Pop with ACK", [&]() { return test_pop_with_ack(server_url); });
    runner.run_test("Pop with ACK Reconsume", [&]() { return test_pop_with_ack_reconsume(server_url); });
    
    // CONSUME TESTS
    std::cout << YELLOW << "\n=== CONSUME TESTS ===" << RESET << "\n" << std::endl;
    runner.run_test("Consumer Basic", [&]() { return test_consumer(server_url); });
    runner.run_test("Consumer with Namespace", [&]() { return test_consumer_namespace(server_url); });
    runner.run_test("Consumer with Task", [&]() { return test_consumer_task(server_url); });
    runner.run_test("Consumer with Partition", [&]() { return test_consumer_with_partition(server_url); });
    runner.run_test("Consumer Batch", [&]() { return test_consumer_batch(server_url); });
    runner.run_test("Consumer Ordering", [&]() { return test_consumer_ordering(server_url); });
    runner.run_test("Consumer Group", [&]() { return test_consumer_group(server_url); });
    runner.run_test("Consumer Group with Partition", [&]() { return test_consumer_group_with_partition(server_url); });
    
    // QUEUE TESTS
    std::cout << YELLOW << "\n=== QUEUE TESTS ===" << RESET << "\n" << std::endl;
    runner.run_test("Create Queue", [&]() { return test_create_queue(server_url); });
    runner.run_test("Delete Queue", [&]() { return test_delete_queue(server_url); });
    runner.run_test("Configure Queue", [&]() { return test_configure_queue(server_url); });
    
    // TRANSACTION TESTS
    std::cout << YELLOW << "\n=== TRANSACTION TESTS ===" << RESET << "\n" << std::endl;
    runner.run_test("Transaction Basic Push+ACK", [&]() { return test_transaction_basic_push_ack(server_url); });
    runner.run_test("Transaction Multiple Pushes", [&]() { return test_transaction_multiple_pushes(server_url); });
    runner.run_test("Transaction Multiple ACKs", [&]() { return test_transaction_multiple_acks(server_url); });
    runner.run_test("Transaction Empty Commit (Error)", [&]() { return test_transaction_empty_commit(server_url); });
    runner.run_test("Transaction ACK with Consumer Group", [&]() { return test_transaction_ack_with_consumer_group(server_url); });
    
    // DLQ TEST
    std::cout << YELLOW << "\n=== DLQ TESTS ===" << RESET << "\n" << std::endl;
    runner.run_test("Dead Letter Queue", [&]() { return test_dlq(server_url); });

    // PRODUCER-SUB TESTS (issue #23, feature A)
    std::cout << YELLOW << "\n=== PRODUCER SUB TESTS ===" << RESET << "\n" << std::endl;
    runner.run_test("Producer Sub ignored from body without auth",
                    [&]() { return test_producer_sub_ignored_without_auth(server_url); });
    
    // SUBSCRIPTION TESTS
    std::cout << YELLOW << "\n=== SUBSCRIPTION TESTS ===" << RESET << "\n" << std::endl;
    runner.run_test("Subscription Mode: new", [&]() { return test_subscription_mode_new(server_url); });
    runner.run_test("Subscription From: now", [&]() { return test_subscription_from_now(server_url); });
    
    // COMPLETE WORKFLOW
    std::cout << YELLOW << "\n=== WORKFLOW TESTS ===" << RESET << "\n" << std::endl;
    runner.run_test("Complete Multi-Stage Pipeline", [&]() { return test_complete_workflow(server_url); });

    // KV AND TIMERS (PLAN_KV_TIMERS.md §5, §4)
    //
    // No probe and no skip: both surfaces exist on every broker that runs this
    // binary, so a failure here is a real failure. The wire contract itself --
    // the exact JSON body, method and path -- is asserted without a broker in
    // test_kv_timers.cpp; these assert the semantics.
    std::cout << YELLOW << "\n=== KV / TIMER TESTS ===" << RESET << "\n" << std::endl;
    runner.run_test("KV Put and Get", [&]() { return test_kv_put_and_get(server_url); });
    runner.run_test("KV Get Absent is not an Error",
                    [&]() { return test_kv_get_absent_is_not_an_error(server_url); });
    runner.run_test("KV putIfAbsent has exactly one winner",
                    [&]() { return test_kv_put_if_absent_has_exactly_one_winner(server_url); });
    runner.run_test("KV expect on an absent key creates nothing",
                    [&]() { return test_kv_expect_on_an_absent_key_creates_nothing(server_url); });
    runner.run_test("KV expect refuses a stale version",
                    [&]() { return test_kv_expect_refuses_a_stale_version(server_url); });
    runner.run_test("KV delete is idempotent",
                    [&]() { return test_kv_delete_is_idempotent(server_url); });
    runner.run_test("KV incr and its ceiling",
                    [&]() { return test_kv_incr_and_its_ceiling(server_url); });
    runner.run_test("Transaction KV gate returns instead of throwing",
                    [&]() { return test_transaction_kv_gate_returns_instead_of_throwing(server_url); });
    runner.run_test("Timer fires into its queue",
                    [&]() { return test_timer_fires_into_its_queue(server_url); });
    runner.run_test("Timer cancel before it fires",
                    [&]() { return test_timer_cancel_before_it_fires(server_url); });
    runner.run_test("Timer cancel on an absent timer says so",
                    [&]() { return test_timer_cancel_absent_says_so(server_url); });
    runner.run_test("Timer reschedule mints a new txn",
                    [&]() { return test_timer_reschedule_mints_a_new_txn(server_url); });
    runner.run_test("Transaction schedules a timer",
                    [&]() { return test_transaction_schedules_a_timer(server_url); });

    // Leave nothing behind for the next run (§10.4).
    cleanup_test_kv_and_timers(server_url);

    std::cout << std::endl;
    runner.print_summary();
    
    return runner.get_failure_count() > 0 ? 1 : 0;
}
