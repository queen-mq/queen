# Queen C++ Client - Quick Start Guide

## 🚀 Get Started in 5 Minutes

### Step 1: Install cpp-httplib

```bash
# macOS
brew install cpp-httplib

# Or download manually to clients/client-cpp directory
cd clients/client-cpp
curl -o httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h
```

### Step 2: Build Test Suite

```bash
cd /Users/alice/Work/queen/clients/client-cpp
make test
```

Expected output:
```
Found cpp-httplib at: /opt/homebrew/include/
SSL support enabled
g++ -std=c++17 -Wall -Wextra -O2 -pthread -DCPPHTTPLIB_OPENSSL_SUPPORT ...
Test suite built successfully!
Run with: ./test_client [server_url]
```

The proxy contract suite (bearer auth, 429 backoff, terminal 403) needs no
server at all — it serves its own responses in-process:

```bash
make run-retry429
```

### Step 3: Start Queen Server

```bash
# In a new terminal
cd /Users/alice/Work/queen/server
./bin/queen-server
```

Wait for:
```
✅ Queen server is ready and listening
```

### Step 4: Run Tests

```bash
# Back in client-cpp directory
./bin/test_client
```

Expected output:
```
========================================
Queen C++ Client Test Suite
========================================
Server URL: http://localhost:6632
========================================

Running: UUID Generation
✓ PASS: UUID Generation

Running: Push Message
✓ PASS: Push Message

... (13 tests total)

========================================
Test Summary
========================================
Total:  13
Passed: 13
Failed: 0
========================================
```

### Step 5: Try the Example

```bash
# Build example
make example

# Run example
./bin/example_basic
```

Expected output:
```
========================================
Queen C++ Client - Basic Example
========================================

✓ Connected to Queen server
✓ Queue created: {"configured":true}

Pushing messages...
  Message 1 pushed
  Message 2 pushed
  Message 3 pushed
  Message 4 pushed
  Message 5 pushed

Consuming messages...
  [1] Received: Hello from C++! (index: 1)
  [2] Received: Hello from C++! (index: 2)
  [3] Received: Hello from C++! (index: 3)
  [4] Received: Hello from C++! (index: 4)
  [5] Received: Hello from C++! (index: 5)

✓ Successfully consumed 5 messages

Cleaning up...
✓ Client closed

========================================
Example completed successfully!
========================================
```

## 📝 Your First Program

Create `my_first_queen.cpp`:

```cpp
#include "queen_client.hpp"
#include <iostream>

using namespace queen;

int main() {
    // Connect to Queen
    QueenClient client("http://localhost:6632");
    
    // Create a queue
    client.queue("hello").create();
    std::cout << "Queue created!" << std::endl;
    
    // Push a message
    client.queue("hello").push({
        {{"data", {{"message", "Hello, Queen!"}}}}
    });
    std::cout << "Message pushed!" << std::endl;
    
    // Pop the message
    auto messages = client.queue("hello").batch(1).wait(false).pop();
    
    if (!messages.empty()) {
        std::cout << "Received: " << messages[0]["data"]["message"] 
                  << std::endl;
    }
    
    // Clean up
    client.close();
    return 0;
}
```

Compile and run:

```bash
g++ -std=c++17 -I. -I../server/vendor -I../server/include \
    my_first_queen.cpp -o my_first_queen -lpthread

./my_first_queen
```

## 🎯 Common Patterns

### Pattern 1: Producer

```cpp
QueenClient client("http://localhost:6632");

client.queue("tasks").create();

// High-throughput push with buffering
BufferOptions buffer;
buffer.message_count = 100;
buffer.time_millis = 1000;

for (int i = 0; i < 10000; i++) {
    client.queue("tasks")
        .buffer(buffer)
        .push({{{"data", {{"id", i}}}}});
}

client.flush_all_buffers();
```

### Pattern 2: Consumer

```cpp
QueenClient client("http://localhost:6632");

// Long-running consumer
client.queue("tasks")
    .concurrency(4)
    .batch(10)
    .consume([](const json& messages) {
        for (const auto& msg : messages) {
            process(msg["data"]);
        }
    });
```

### Pattern 3: Pipeline with Transactions

```cpp
QueenClient client("http://localhost:6632");

client.queue("input").create();
client.queue("output").create();

client.queue("input")
    .auto_ack(false)
    .consume([&](const json& msg) {
        // Process
        auto result = transform(msg["data"]);
        
        // Atomic ack + push
        client.transaction()
            .ack(msg)
            .queue("output")
            .push({{{"data", result}}})
            .commit();
    });
```

## 🔧 Troubleshooting

### Error: "cpp-httplib not found"

**Solution:**
```bash
brew install cpp-httplib
# Or
curl -o httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h
```

### Error: "Connection refused"

**Solution:** Make sure Queen server is running:
```bash
cd ../server
./bin/queen-server
```

### Error: "undefined reference to pthread"

**Solution:** Add `-lpthread` to your compile command:
```bash
g++ ... -lpthread
```

### Tests failing?

**Check:**
1. Is server running? `curl http://localhost:6632/health`
2. Is port 6632 available? `lsof -i :6632`
3. Try: `./test_client http://localhost:6632`

## 📚 Next Steps

1. **Read the README** - Full API documentation
   ```bash
   cat README.md
   ```

2. **Check Implementation Summary** - Architecture details
   ```bash
   cat IMPLEMENTATION_SUMMARY.md
   ```

3. **Run with logging** - Debug your code
   ```bash
   export QUEEN_CLIENT_LOG=true
   ./test_client
   ```

4. **Explore examples** - More usage patterns
   ```bash
   # Coming soon: More examples in examples/ directory
   ```

## 🎉 You're All Set!

You now have a fully functional C++ client for Queen Message Queue. Start building!

```cpp
QueenClient client("http://localhost:6632");
// Your amazing distributed system starts here! 🚀
```

**Happy Queuing! 👑**

