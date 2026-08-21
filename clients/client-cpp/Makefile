# Queen C++ Client - Makefile
# Builds test suite and examples

CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -pthread

# Include paths
INCLUDES = -I. \
           -I../server/vendor \
           -I../server/include

# Check for cpp-httplib installation
# Try Homebrew path first (macOS), then system paths
HTTPLIB_PATHS = /opt/homebrew/include \
                /usr/local/include \
                /usr/include
				
HTTPLIB_FOUND = $(foreach path,$(HTTPLIB_PATHS),$(wildcard $(path)/httplib.h))

ifneq ($(HTTPLIB_FOUND),)
    INCLUDES += -I$(dir $(firstword $(HTTPLIB_FOUND)))
    $(info Found cpp-httplib at: $(dir $(firstword $(HTTPLIB_FOUND))))
else
    $(warning cpp-httplib not found in system paths. Make sure it's installed or in current directory)
    $(warning Run: brew install cpp-httplib  (macOS))
    $(warning Or download: curl -o httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h)
endif

# Libraries
LIBS = -lpthread

# SSL support (optional but recommended)
USE_SSL ?= 1
ifeq ($(USE_SSL),1)
    CXXFLAGS += -DCPPHTTPLIB_OPENSSL_SUPPORT
    
    # Detect OpenSSL installation
    OPENSSL_PREFIX := $(shell brew --prefix openssl 2>/dev/null)
    ifneq ($(OPENSSL_PREFIX),)
        INCLUDES += -I$(OPENSSL_PREFIX)/include
        LIBS += -L$(OPENSSL_PREFIX)/lib -lssl -lcrypto
        $(info SSL support enabled with OpenSSL at: $(OPENSSL_PREFIX))
    else
        LIBS += -lssl -lcrypto
        $(info SSL support enabled (using system OpenSSL))
    endif
else
    $(info SSL support disabled. Set USE_SSL=1 to enable)
endif

# Targets
.PHONY: all test retry429 run-retry429 kvtimers run-kvtimers conflation run-conflation ephemeral run-ephemeral unit run-unit clean help

all: test

# Build test suite
test: bin/test_client
	@echo "Test suite built successfully!"
	@echo "Run with: ./bin/test_client [server_url]"
	@echo "Example: ./bin/test_client http://localhost:6632"

bin/test_client: test_client.cpp queen_client.hpp ../server/vendor/json.hpp ../server/include/threadpool.hpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) test_client.cpp -o bin/test_client $(LIBS)

# Build proxy contract test suite (bearer auth, 429 backoff, terminal 403).
# Self-contained: serves its own responses from an in-process httplib::Server,
# so it needs neither a broker nor Postgres.
retry429: bin/test_retry429
	@echo "Proxy contract test suite built successfully!"
	@echo "Run with: ./bin/test_retry429"

bin/test_retry429: test_retry429.cpp queen_client.hpp ../server/vendor/json.hpp ../server/include/threadpool.hpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) test_retry429.cpp -o bin/test_retry429 $(LIBS)

# Run proxy contract tests (no server required)
run-retry429: bin/test_retry429
	./bin/test_retry429

# Build the KV / timers wire-contract suite (PLAN_KV_TIMERS.md §5, §4, §6.3,
# §8.1/§8.3, §9.6). Like retry429 it serves its own responses in-process, so it
# needs neither a broker nor Postgres -- it asserts the exact JSON body, method
# and path the client puts on the wire, which is the only thing that catches a
# wrong wire shape before production.
kvtimers: bin/test_kv_timers
	@echo "KV/timers wire test suite built successfully!"
	@echo "Run with: ./bin/test_kv_timers"

bin/test_kv_timers: test_kv_timers.cpp queen_client.hpp ../server/vendor/json.hpp ../server/include/threadpool.hpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) test_kv_timers.cpp -o bin/test_kv_timers $(LIBS)

run-kvtimers: bin/test_kv_timers
	./bin/test_kv_timers

# Build the conflation wire-contract suite (PLAN_CONFLATION.md §4, §3.1). Also
# broker-free, and it has to be: the two things it pins -- that an old broker's
# missing "conflation":true echo raises instead of silently draining a backlog,
# and that a declaration conflict warns exactly ONCE per (queue, group) -- are
# both invisible to an end-to-end run against a live 1.1.0 broker, which can
# never produce the first response and cannot count the second.
conflation: bin/test_conflation
	@echo "Conflation wire test suite built successfully!"
	@echo "Run with: ./bin/test_conflation"

bin/test_conflation: test_conflation.cpp queen_client.hpp ../server/vendor/json.hpp ../server/include/threadpool.hpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) test_conflation.cpp -o bin/test_conflation $(LIBS)

run-conflation: bin/test_conflation
	./bin/test_conflation

# Build the ephemeral-queues wire suite (EPHEMERAL_QUEUES.md §3.1, §4, §7.3).
# Broker-free like its siblings, and it has to be: the thing it pins hardest --
# that a 404 from a pre-1.1 broker OR from a proxy that fails closed on unknown
# API paths becomes ONE clear "upgrade" error rather than reading as "your queue
# is missing" -- cannot be produced by any run against a broker that has the
# routes.
ephemeral: bin/test_ephemeral
	@echo "Ephemeral wire test suite built successfully!"
	@echo "Run with: ./bin/test_ephemeral"

bin/test_ephemeral: test_ephemeral.cpp queen_client.hpp ../server/vendor/json.hpp ../server/include/threadpool.hpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) test_ephemeral.cpp -o bin/test_ephemeral $(LIBS)

run-ephemeral: bin/test_ephemeral
	./bin/test_ephemeral

# Every suite that needs no broker, in one command.
unit: retry429 kvtimers conflation ephemeral

run-unit: run-retry429 run-kvtimers run-conflation run-ephemeral

# Run tests (requires Queen server running)
run-test: bin/test_client
	@echo "========================================" 
	@echo "Running Queen C++ Client Tests"
	@echo "========================================"
	@echo "Note: Ensure Queen server is running on localhost:6632"
	@echo ""
	./bin/test_client

# Run tests with custom server URL
run-test-custom: bin/test_client
	@echo "Running tests against: $(SERVER_URL)"
	./bin/test_client $(SERVER_URL)

# Build basic example
example: bin/example_basic
	@echo "Example built successfully!"
	@echo "Run with: ./bin/example_basic"

bin/example_basic: example_basic.cpp queen_client.hpp ../server/vendor/json.hpp ../server/include/threadpool.hpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) example_basic.cpp -o bin/example_basic $(LIBS)
	@echo "✓ Example built in bin/example_basic"

# Example programs (for future expansion)
examples: example_basic example_consumer

example_basic: bin/example_basic

example_consumer: examples/consumer.cpp queen_client.hpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) examples/consumer.cpp -o bin/example_consumer $(LIBS)

# Clean build artifacts
clean:
	rm -f test_client example_basic  # Legacy locations
	rm -rf bin
	rm -f *.o
	@echo "Cleaned build artifacts"

# Help
help:
	@echo "Queen C++ Client - Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  make              - Build test suite (default)"
	@echo "  make test         - Build test suite"
	@echo "  make run-test     - Build and run tests (server must be running)"
	@echo "  make retry429     - Build proxy contract tests (bearer/429/403)"
	@echo "  make run-retry429 - Build and run proxy contract tests (no server needed)"
	@echo "  make kvtimers     - Build KV/timer wire contract tests"
	@echo "  make run-kvtimers - Build and run KV/timer wire tests (no server needed)"
	@echo "  make conflation   - Build conflation wire contract tests"
	@echo "  make run-conflation - Build and run conflation wire tests (no server needed)"
	@echo "  make ephemeral    - Build ephemeral-queue wire contract tests"
	@echo "  make run-ephemeral - Build and run ephemeral wire tests (no server needed)"
	@echo "  make unit         - Build every suite that needs no broker"
	@echo "  make run-unit     - Build and run every suite that needs no broker"
	@echo "  make clean        - Remove build artifacts"
	@echo "  make help         - Show this help message"
	@echo ""
	@echo "Options:"
	@echo "  USE_SSL=1         - Enable SSL support (default: 1)"
	@echo "  SERVER_URL=...    - Custom server URL for tests"
	@echo ""
	@echo "Examples:"
	@echo "  make test"
	@echo "  make run-test"
	@echo "  make run-test SERVER_URL=http://my-server:6632"
	@echo "  make clean"
	@echo ""
	@echo "Dependencies:"
	@echo "  - C++17 compiler (g++ or clang++)"
	@echo "  - cpp-httplib (header-only HTTP client)"
	@echo "  - OpenSSL (optional, for HTTPS support)"
	@echo ""
	@echo "Install cpp-httplib:"
	@echo "  macOS:   brew install cpp-httplib"
	@echo "  Manual:  curl -o httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h"

