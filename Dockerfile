# Multi-stage Dockerfile for Queen Message Queue
#
# Builds the complete Queen stack:
# - Rust broker (server/): one binary, storage in its own data directory
# - the proxy and its console, linked into the broker (in-process with
#   QUEEN_PROXY_EMBEDDED=true), and the Kafka facade (QUEEN_KAFKA_EMBEDDED=true)
# - Vue.js frontend dashboard (served by the broker's SPA fallback)
# - queenctl operator CLI (Go static binary)
#
# Build: DOCKER_BUILDKIT=1 docker build -t queen-mq .
# Run:   docker run -p 6632:6632 -v queen-data:/var/lib/queen/raft queen-mq
#
# Operator CLI access from inside the container:
#   docker exec -it queen queenctl status      # zero-config: uses localhost:6632
#   docker exec -it queen queenctl tail orders --cg debug --follow
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

# Stage 1b: Build the proxy console
#
# The proxy crate is linked into the broker by the default `server` feature (the
# single binary, PLAN_SINGLE_BINARY.md W3/W4: QUEEN_PROXY_EMBEDDED=true runs it
# in-process, server/src/proxy_embed.rs). It embeds console/dist with rust_embed,
# which hard-errors at compile time when the folder is missing, and
# .dockerignore keeps the local dist out of the context — so it is built here.
FROM node:24-alpine AS console-builder

WORKDIR /build/console

COPY proxy/console/package*.json ./

RUN npm ci

COPY proxy/console/ ./

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
# ...and the Kafka facade library, linked into the broker by the default `kafka`
# feature (server/src/kafka_inproc.rs runs it in-process). A path dependency
# like queen-protocol, so it has to be in the context here too.
COPY protocols/queen-kafka/Cargo.toml /usr/build/protocols/queen-kafka/Cargo.toml
COPY protocols/queen-kafka/src /usr/build/protocols/queen-kafka/src
# ...and the proxy library, linked in by the default `server` feature (stage 1b).
# Its console is rust_embed-ed, so it has to be here. Its second embed,
# ../server/webapp/dist, is the dashboard that layer 3 below puts at
# /usr/build/server/webapp/dist.
COPY proxy/Cargo.toml /usr/build/proxy/Cargo.toml
COPY proxy/src /usr/build/proxy/src
COPY --from=console-builder /build/console/dist /usr/build/proxy/console/dist

# Layer 1: manifests + build script + version file (build.rs embeds
# server.json's version into the binary via env!("QUEEN_VERSION")).
COPY server/Cargo.toml server/Cargo.lock server/server.json server/build.rs ./

# Layer 2: source.
COPY server/src ./src

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

# Stage 3: Build queenctl (Go operator CLI)
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

# Stage 4: Runtime Image
FROM ubuntu:24.04

# Runtime dependencies.
RUN sed -i -e 's|security.ubuntu.com|mirrors.edge.kernel.org|g' -e 's|archive.ubuntu.com|mirrors.edge.kernel.org|g' /etc/apt/sources.list.d/ubuntu.sources || true \
    && apt-get update && apt-get install -y \
    libssl3 \
    zlib1g \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# The broker binary: the one process of a node.
COPY --from=server-builder /queen ./bin/queen

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

# The Kafka listener of the in-process facade. Documentation only (EXPOSE
# publishes nothing on its own) and only reachable with QUEEN_KAFKA_EMBEDDED=true;
# 9092 because that is the port every Kafka client's default bootstrap.servers
# names. Remember QUEEN_KAFKA_ADVERTISED_ADDR: a container that advertises its
# internal address is a bootstrap that succeeds and a produce that hangs.
EXPOSE 9092

# Run the Rust broker
CMD ["./bin/queen"]
