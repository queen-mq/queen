# C++ client test runner. Build context = repo root.
#   docker build -f test/runners/cpp/Dockerfile -t queen-test-runner-cpp .
#
# The C++ client's Makefile expects three headers that are gitignored / not in
# the tree. Two are fetched here at pinned versions; threadpool.hpp (MIT) can
# only be recovered from git history, so it is vendored into test/vendor/cpp/.
FROM ubuntu:24.04

RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ make libssl-dev curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# The Makefile's -I../server/{vendor,include} resolve to clients/server/* here.
RUN mkdir -p clients/server/vendor clients/server/include \
 && curl -fsSL https://raw.githubusercontent.com/nlohmann/json/v3.11.3/single_include/nlohmann/json.hpp \
        -o clients/server/vendor/json.hpp \
 && curl -fsSL https://raw.githubusercontent.com/yhirose/cpp-httplib/v0.27.0/httplib.h \
        -o /usr/local/include/httplib.h

# threadpool.hpp is vendored (recovered from history; astp ThreadPool, MIT).
COPY test/vendor/cpp/threadpool.hpp clients/server/include/threadpool.hpp
COPY clients/client-cpp ./clients/client-cpp

# make test = compile bin/test_client only (it does not run the binary).
RUN cd clients/client-cpp && make test

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/cpp/entrypoint.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
