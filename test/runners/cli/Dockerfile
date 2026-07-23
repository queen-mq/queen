# queenctl CLI E2E runner. Build context = repo root.
#   docker build -f test/runners/cli/Dockerfile -t queen-test-runner-cli .
#
# This suite REQUIRES the Go workspace: the CLI references QueueConfig fields
# (RetentionEnabled, DeadLetterQueue, ...) that exist only in the local,
# unpublished clients/client-go — go.work wires them in. GOWORK=off would fail
# to build. So we copy go.work + BOTH modules and do NOT disable the workspace.
FROM golang:1.24-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends make curl ca-certificates git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY go.work ./
COPY clients/client-go ./clients/client-go
COPY clients/client-cli ./clients/client-cli

# Warm the module cache, then build bin/queenctl (the tests stat it and fail
# setup if missing). Workspace resolution supplies the local client-go.
RUN cd clients/client-cli && go mod download
RUN cd clients/client-cli && make build

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/cli/entrypoint.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
