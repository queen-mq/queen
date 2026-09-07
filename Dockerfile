# Multi-stage Dockerfile for Queen Message Queue
#
# Builds the complete Queen stack:
# - Rust broker (server/, segments-only engine; SQL schema baked in via include_str!)
# - Vue.js frontend dashboard (served by the broker's SPA fallback)
# - queenctl operator CLI (Go static binary)
#
# Build: DOCKER_BUILDKIT=1 docker build -t queen-mq .
# Run:   docker run -p 6632:6632 -e PG_HOST=your-db queen-mq
#
# Operator CLI access from inside the container:
#   docker exec -it queen queenctl status      # zero-config: uses localhost:6632
#   docker exec -it queen queenctl tail orders --cg debug --follow
#
# For full stack with PostgreSQL, use docker-compose.yml
#
# Requires BuildKit: DOCKER_BUILDKIT=1 docker build -t queen-mq .
#
# Stage 1: Build Frontend
FROM node:24-alpine AS frontend-builder

WORKDIR /app/webapp

# Copy frontend package files
COPY app/package*.json ./

# Install dependencies
RUN npm ci

# Copy frontend source
COPY app/ ./

# Build frontend. vite writes to ../server/webapp/dist relative to the app
# source root (app/vite.config.js) — the one path both Rust binaries embed —
# which with this WORKDIR lands at /app/server/webapp/dist.
RUN npm run build

# Stage 2: Build the Rust broker
FROM rust:1-bookworm AS server-builder
# TARGETARCH folds the platform into the cache ids below: a multi-platform build
# runs this stage once per platform CONCURRENTLY, and two cargo processes
# unpacking one registry (or writing one target/) race — `.cargo-ok` /
# `File exists (os error 17)` at the first crate, before any Queen code compiles.
# Per-platform ids give each half its own cache; nothing is shared across arches.
ARG TARGETARCH

WORKDIR /usr/build/server

# Layer 0: the shared wire-type crate. server/Cargo.toml takes queen-protocol as
# a DEV dependency (the conformance tests only — the release binary never links
# it), but cargo resolves the entire dependency graph, dev dependencies
# included, before it compiles anything. So the path has to exist even here.
COPY crates /usr/build/crates

# Layer 1: manifests + build script + version file (build.rs embeds
# server.json's version into the binary via env!("QUEEN_VERSION")).
COPY server/Cargo.toml server/Cargo.lock server/server.json server/build.rs ./

# Layer 2: source + the SQL schema (embedded into the binary with include_str!).
COPY server/src ./src
COPY server/sql ./sql

# Layer 3: the built dashboard. server/src/handlers/static_files.rs embeds
# `webapp/dist` with rust_embed, which hard-errors at compile time when the
# folder is missing — so this COPY is a build dependency, not packaging.
COPY --from=frontend-builder /app/server/webapp/dist ./webapp/dist

# Build. Cargo registry + target dirs are BuildKit caches; copy the binary out of
# the (non-persisted) target cache so it lands in the image layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-server-${TARGETARCH} \
    --mount=type=cache,target=/usr/build/server/target,id=target-server-${TARGETARCH} \
    cargo build --release && cp target/release/queen /queen

# Verify
RUN test -f /queen && echo "Build successful"

# Stage 3: Build the queen-kafka facade (Kafka wire protocol front)
#
# Its own stage and its own binary, because that is what the deployment is:
# EMBEDDED MODE (server/src/kafka_facade.rs) has the broker SPAWN this file as a
# supervised child process, so the image ships two binaries and one process tree.
# It is inert unless QUEEN_KAFKA_EMBEDDED=true, which is why it can be added to
# the default image without changing what the default image does.
#
# No frontend stage feeds this one and no path dependency reaches out of the
# directory (protocols/queen-kafka/Cargo.toml has none), so the context is the
# crate alone.
FROM rust:1-bookworm AS kafka-builder
# TARGETARCH folds the platform into the cache ids below: a multi-platform build
# runs this stage once per platform CONCURRENTLY, and two cargo processes
# unpacking one registry (or writing one target/) race — `.cargo-ok` /
# `File exists (os error 17)` at the first crate, before any Queen code compiles.
# Per-platform ids give each half its own cache; nothing is shared across arches.
ARG TARGETARCH

WORKDIR /usr/build/queen-kafka

# Layer 1: manifests. Cargo.lock is copied so the image builds the versions the
# repository tested, exactly as the server stage above does.
COPY protocols/queen-kafka/Cargo.toml protocols/queen-kafka/Cargo.lock ./

# Layer 2: source.
COPY protocols/queen-kafka/src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-kafka-${TARGETARCH} \
    --mount=type=cache,target=/usr/build/queen-kafka/target,id=target-kafka-${TARGETARCH} \
    cargo build --release && cp target/release/queen-kafka /queen-kafka

RUN test -f /queen-kafka && echo "Facade build successful"

# Stage 4: Build the queen-sqs facade (SQS/SNS wire protocol front)
#
# The twin of the stage above, for the same reason: EMBEDDED MODE
# (server/src/sqs_facade.rs) has the broker SPAWN this file as a supervised child
# process, so the image ships the binary and one process tree carries both. It is
# inert unless QUEEN_SQS_EMBEDDED=true, which is why it can be added to the
# default image without changing what the default image does.
#
# No frontend stage feeds this one and no path dependency reaches out of the
# directory (protocols/queen-sqs/Cargo.toml has none), so the context is the
# crate alone.
FROM rust:1-bookworm AS sqs-builder
# TARGETARCH folds the platform into the cache ids below: a multi-platform build
# runs this stage once per platform CONCURRENTLY, and two cargo processes
# unpacking one registry (or writing one target/) race — `.cargo-ok` /
# `File exists (os error 17)` at the first crate, before any Queen code compiles.
# Per-platform ids give each half its own cache; nothing is shared across arches.
ARG TARGETARCH

WORKDIR /usr/build/queen-sqs

# Layer 1: manifests. Cargo.lock is copied so the image builds the versions the
# repository tested, exactly as the two stages above do.
COPY protocols/queen-sqs/Cargo.toml protocols/queen-sqs/Cargo.lock ./

# Layer 2: source.
COPY protocols/queen-sqs/src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-sqs-${TARGETARCH} \
    --mount=type=cache,target=/usr/build/queen-sqs/target,id=target-sqs-${TARGETARCH} \
    cargo build --release && cp target/release/queen-sqs /queen-sqs

RUN test -f /queen-sqs && echo "SQS facade build successful"

# Stage 5: Build the queen-s3 sink connector (S3 / data-lake sink)
#
# The third binary that rides beside the broker, on the same contract as the two
# facades above: EMBEDDED MODE (server/src/s3_sink.rs) has the broker SPAWN this
# file as a supervised child process, so the image ships it and one process tree
# carries them all. It is inert unless QUEEN_S3_EMBEDDED=true, which is why it can
# be added to the default image without changing what the default image does.
#
# It is NOT a wire-protocol facade: nothing connects TO it. It is a Queen client
# that reads the log through POST /api/v1/fetch and writes JSONL or Parquet
# objects to an object store (PLAN_S3_SINK.md §3).
#
# No frontend stage feeds this one and no path dependency reaches out of the
# directory (connectors/queen-s3/Cargo.toml has none), so the context is the
# crate alone.
FROM rust:1-bookworm AS s3-builder
# TARGETARCH folds the platform into the cache ids below: a multi-platform build
# runs this stage once per platform CONCURRENTLY, and two cargo processes
# unpacking one registry (or writing one target/) race — `.cargo-ok` /
# `File exists (os error 17)` at the first crate, before any Queen code compiles.
# Per-platform ids give each half its own cache; nothing is shared across arches.
ARG TARGETARCH

WORKDIR /usr/build/queen-s3

# Layer 1: manifests. Cargo.lock is copied so the image builds the versions the
# repository tested, exactly as the three stages above do.
COPY connectors/queen-s3/Cargo.toml connectors/queen-s3/Cargo.lock ./

# Layer 2: source.
COPY connectors/queen-s3/src ./src

# Its OWN registry cache id. The four Rust stages build in parallel under
# BuildKit and a shared cache mount is a shared lock: `cargo-registry-s3` keeps
# this stage off the other three's mount rather than serialising all of them
# behind one directory.
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-s3-${TARGETARCH} \
    --mount=type=cache,target=/usr/build/queen-s3/target,id=target-s3-${TARGETARCH} \
    cargo build --release && cp target/release/queen-s3 /queen-s3

RUN test -f /queen-s3 && echo "S3 sink build successful"

# Stage 6: Build queenctl (Go operator CLI)
FROM golang:1.24-alpine AS cli-builder

# Embed broker version + commit + build date into the binary so
# `queenctl version` reports the same string the broker does.
ARG QUEENCTL_VERSION=dev
ARG QUEENCTL_COMMIT=none

WORKDIR /src

# Copy only the two Go modules queenctl needs, plus the workspace file that
# binds them together. go.work is NOT optional here: client-cli/go.mod requires
# `client-go v0.15.0` from the public proxy, and the local `replace` directive
# that used to override it was deliberately removed (it broke downstream
# `go install`) in favour of this workspace. Without go.work the build resolves
# the published v0.15.0, which predates HTTPError.Code and three QueueConfig
# fields the CLI uses, and the compile fails. The paths inside go.work are
# ./clients/... relative to the workspace root, which is why it lands in /src.
COPY go.work ./
COPY clients/client-go/ ./clients/client-go/
COPY clients/client-cli/ ./clients/client-cli/

WORKDIR /src/clients/client-cli

# Pure-Go static build, no CGO so the binary runs on the ubuntu:24.04
# runtime stage (and on scratch images for that matter).
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOFLAGS=-trimpath go build \
        -ldflags "-s -w \
            -X 'github.com/smartpricing/queen/clients/client-cli/cmd.BuildVersion=${QUEENCTL_VERSION}' \
            -X 'github.com/smartpricing/queen/clients/client-cli/cmd.BuildCommit=${QUEENCTL_COMMIT}' \
            -X 'github.com/smartpricing/queen/clients/client-cli/cmd.BuildDate=docker'" \
        -o /out/queenctl .

# Sanity check: the binary must run without any dynamic deps.
RUN /out/queenctl version --short

# Stage 7: Runtime Image
FROM ubuntu:24.04

# Runtime dependencies + PostgreSQL 18 client tools (pg_dump, pg_restore for
# operator use). The PGDG repo is required because Ubuntu 24.04 only ships PG 16.
RUN sed -i -e 's|security.ubuntu.com|mirrors.edge.kernel.org|g' -e 's|archive.ubuntu.com|mirrors.edge.kernel.org|g' /etc/apt/sources.list.d/ubuntu.sources || true \
    && apt-get update && apt-get install -y \
    libssl3 \
    zlib1g \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
       | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt \
       $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y postgresql-client-18 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Rust broker binary (SQL schema is compiled in — no schema files to copy).
COPY --from=server-builder /queen ./bin/queen

# The Kafka facade, NEXT TO the broker binary — that adjacency is the contract:
# with QUEEN_KAFKA_EMBEDDED=true and no QUEEN_KAFKA_BIN, the supervisor resolves
# the child from the directory of its own executable (kafka_facade::resolve_bin),
# so embedded mode works in this image with zero extra deployment. Run it alone
# instead with `docker run ... queen-mq ./bin/queen-kafka`.
COPY --from=kafka-builder /queen-kafka ./bin/queen-kafka

# The SQS facade, on the same adjacency contract: with QUEEN_SQS_EMBEDDED=true and
# no QUEEN_SQS_BIN, the supervisor resolves the child from the directory of its own
# executable (sqs_facade::resolve_bin). Run it alone instead with
# `docker run ... queen-mq ./bin/queen-sqs`.
COPY --from=sqs-builder /queen-sqs ./bin/queen-sqs

# The S3 sink, on the same adjacency contract: with QUEEN_S3_EMBEDDED=true and no
# QUEEN_S3_BIN, the supervisor resolves the child from the directory of its own
# executable (s3_sink::resolve_bin). Run it alone instead with
# `docker run ... queen-mq ./bin/queen-s3` — which is the shape that scales out,
# because a sink is a client and needs nothing from the broker's container.
COPY --from=s3-builder /queen-s3 ./bin/queen-s3

# The same dashboard bytes the binary already embeds, on disk for inspection.
# The binary does not read them: nothing in server/src implements a
# static-dir override, so this is a copy for humans, not a serving path.
COPY --from=frontend-builder /app/server/webapp/dist ./webapp/dist

# The queenctl operator CLI onto $PATH. With QUEEN_SERVER pre-set below, an
# in-container invocation needs no flags:  docker exec -it queen queenctl status
COPY --from=cli-builder /out/queenctl /usr/local/bin/queenctl

# In-container default for queenctl. Overridden by --server or `docker run -e ...`.
ENV QUEEN_SERVER=http://localhost:6632
# QUEEN_STATIC_DIR is deliberately NOT set: no code reads it (the dashboard is
# compiled into the binary), and an env var that configures nothing is a lie to
# whoever tries to point it somewhere.

# Expose the broker port
EXPOSE 6632

# The Kafka listener of the embedded facade. Documentation only (EXPOSE publishes
# nothing on its own) and only reachable with QUEEN_KAFKA_EMBEDDED=true; 9092
# because that is the port every Kafka client's default bootstrap.servers names.
# Remember QUEEN_KAFKA_ADVERTISED_ADDR: a container that advertises its internal
# address is a bootstrap that succeeds and a produce that hangs.
EXPOSE 9092

# The SQS listener of the embedded facade. Documentation only, and only reachable
# with QUEEN_SQS_EMBEDDED=true; 9324 because that is the de-facto self-hosted SQS
# port every "point boto3 at a local queue" configuration already names.
# Remember QUEEN_SQS_CREDENTIALS: SigV4 is the default mode and a facade started
# without keys answers every request InvalidClientTokenId (the broker refuses that
# combination at boot rather than letting it crash-loop).
EXPOSE 9324

# The S3 sink gets NO EXPOSE, deliberately. Nothing connects to it — it is a
# client of the broker and of an object store, in that direction only — and its
# one listener (QUEEN_S3_LISTEN, /healthz and /metrics) defaults to 127.0.0.1,
# which an EXPOSE would advertise as reachable when it is not. A deployment that
# wants those two endpoints scraped sets QUEEN_S3_LISTEN to 0.0.0.0:9333 and
# publishes the port itself.

# Run the Rust broker
CMD ["./bin/queen"]
