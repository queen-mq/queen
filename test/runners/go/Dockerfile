# Go client integration runner. Build context = repo root.
#   docker build -f test/runners/go/Dockerfile -t queen-test-runner-go .
FROM golang:1.24-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Build the client-go module standalone. GOWORK=off keeps the repo-root go.work
# from auto-activating and dragging in client-cli (we don't copy it here anyway).
ENV GOWORK=off
WORKDIR /src

COPY clients/client-go/go.mod clients/client-go/go.sum ./
RUN go mod download

COPY clients/client-go/ ./
# Compile both test packages ahead of time to surface build errors early.
RUN go vet ./tests/... ./tests/streams_integration/...

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/go/entrypoint.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
