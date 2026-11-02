# Queen C++ Client - Quick Start

Five minutes from a checkout to a consumer. The [README](README.md) has everything else.

## 1. Start a broker

```bash
docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
```

## 2. Install the one dependency

```bash
brew install cpp-httplib openssl     # macOS; on Debian/Ubuntu: libssl-dev, and httplib.h v0.27.0
```

nlohmann/json comes with the client (`third_party/`).

## 3. Build and run the example

```bash
cd clients/client-cpp
make example
./bin/example_basic http://localhost:6632
```

It pushes five orders into one partition, each with its own dedup key, and consumes them:

```
order 1: queued
...
received {"order":1,"total":10.0}
...
consumed 5 message(s), acked 5
```

Run it again and the pushes answer `duplicate`: the same transaction ids write nothing twice.

## 4. Write your own

```cpp
#include "queen_client.hpp"
#include <iostream>

int main() {
    queen::Queen queen("http://localhost:6632");

    queen.queue("tasks").partition("user-7").push({{"job", "send-email"}}, "email-7-welcome");

    queen.queue("tasks")
        .limit(1)
        .consume([](const queen::Message& m) {   // acked when it returns, retried if it throws
            std::cout << m.data << "\n";
        });

    queen.close();
}
```

```bash
g++ -std=c++17 -O2 -pthread -I clients/client-cpp \
    -I /opt/homebrew/include -I "$(brew --prefix openssl)/include" \
    tasks.cpp -o tasks -L "$(brew --prefix openssl)/lib" -lssl -lcrypto
```

With CMake, link the `queen::client` target instead (see the README's Install section).

## 5. Run the tests

```bash
make run-unit                                     # no broker needed
make test && ./bin/test_client http://localhost:6632
```
