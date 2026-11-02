# C++ client test runner. Build context = repo root.
#   docker build -f test/runners/cpp/Dockerfile -t queen-test-runner-cpp .
#
# The client is clients/client-cpp/queen_client.hpp with nlohmann/json vendored
# beside it (third_party/). The one header it needs from outside is
# cpp-httplib, fetched here at a pinned version; OpenSSL and zstd come from apt.
FROM ubuntu:24.04

RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ make libssl-dev libzstd-dev curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://raw.githubusercontent.com/yhirose/cpp-httplib/v0.27.0/httplib.h \
        -o /usr/local/include/httplib.h

WORKDIR /src
COPY clients/client-cpp ./clients/client-cpp

# make test = compile bin/test_client only (it does not run the binary).
# `unit` builds the broker-free suites: retry429 (the proxy contract --
# bearer/429/403), kvtimers (the KV/timer wire contract), locks (the lock and
# semaphore wire contract; its live half runs from the entrypoint), conflation
# (the flag on the pop, the degrade-loudly error against a pre-1.1.0 broker,
# the once-per-group conflict warning), consumer (the consume path: ack/renew
# results and the consume loop), and the buffer, ephemeral, autopilot, reads and
# wireforms suites (wireforms: the binary push body and the framed pop answer,
# built with zstd from libzstd-dev). All of them serve their own responses
# in-process, so they compile and run without a broker.
RUN cd clients/client-cpp && make test && make unit

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/cpp/entrypoint.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
