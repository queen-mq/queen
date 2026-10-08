# Multi-stage Dockerfile for Queen Message Queue
#
# Builds the complete Queen stack:
# - Rust broker (server/): one binary, storage in its own data directory
# - the proxy and its console, linked into the broker (in-process with
#   QUEEN_PROXY_EMBEDDED=true), the Kafka facade (QUEEN_KAFKA_EMBEDDED=true)
#   and the Postgres connectors (on by default, /api/v1/connectors)
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

# Stage 2a: what the dependency layer of stage 2 is built from
#
# The broker's manifest and lock file and the manifest of each crate it links by
# path, with an empty library in place of every crate's source. Stage 2 copies
# this folder and compiles it: that layer holds every dependency, and it is
# reused until a manifest or the lock file changes.
#
# A cache mount cannot do this in CI. `cache-to` exports layers, not the content
# of a cache mount, so on a fresh runner the mount is empty and every dependency
# compiles again.
#
# The broker's own version is blanked: it changes at every release and no
# dependency is compiled differently for it. With the real number every release
# would compile every dependency again.
FROM rust:1-bookworm AS manifests

WORKDIR /manifests

COPY crates/queen-protocol/Cargo.toml crates/queen-protocol/Cargo.toml
COPY protocols/queen-kafka/Cargo.toml protocols/queen-kafka/Cargo.toml
COPY connectors/queen-s3/Cargo.toml connectors/queen-s3/Cargo.toml
COPY connectors/queen-pg/Cargo.toml connectors/queen-pg/Cargo.toml
COPY proxy/Cargo.toml proxy/Cargo.toml
COPY server/Cargo.toml server/Cargo.lock server/

RUN sed -i '0,/^version = /s/^version = .*/version = "0.0.0"/' server/Cargo.toml \
    && sed -i '/^name = "queen-engine"$/{n;s/^version = .*/version = "0.0.0"/}' server/Cargo.lock \
    && for crate in crates/queen-protocol protocols/queen-kafka connectors/queen-s3 connectors/queen-pg proxy server; do \
        mkdir -p "$crate/src" && : > "$crate/src/lib.rs"; \
    done

# Stage 2: Build the Rust broker
#
# Three layers, each compiled on top of the one before, from what changes least
# to what changes most: the dependencies, the crates linked by path, the broker.
# No cache mounts: a multi-platform build runs this stage once per platform,
# each with its own layers, so nothing is shared across arches.
FROM rust:1-bookworm AS server-builder

WORKDIR /usr/build/server

# Layer 0: the dependencies, from stage 2a. `--lib` and no src/main.rs: no
# binary is ever built from the empty sources, so none can reach the image. The
# rest of the command line is the one of the real build below, so cargo finds
# the dependencies already compiled there.
COPY --from=manifests /manifests/ /usr/build/
RUN cargo build --release --lib

# Layer 1: the crates the broker links by path. First the shared wire-type
# crate, queen-protocol.
COPY crates/queen-protocol/src /usr/build/crates/queen-protocol/src
# ...and the Kafka facade library, linked into the broker by the default `kafka`
# feature (server/src/kafka_inproc.rs runs it in-process).
COPY protocols/queen-kafka/src /usr/build/protocols/queen-kafka/src
# ...and the S3 / data-lake sink library, linked in by the default `s3` feature
# (server/src/s3_inproc.rs runs it in-process).
COPY connectors/queen-s3/src /usr/build/connectors/queen-s3/src
# ...and the Postgres connectors library, linked in by the default `pg` feature
# (server/src/pg_inproc.rs runs every connector in-process). Its own Cargo.lock
# is not needed here, the broker's lock covers it.
COPY connectors/queen-pg/src /usr/build/connectors/queen-pg/src
# ...and the proxy library, linked in by the default `server` feature (stage 1b).
# It embeds two folders with rust_embed, which hard-errors at compile time when
# a folder is missing: its console, and ../server/webapp/dist, the dashboard.
# The broker embeds the same dashboard (server/src/handlers/static_files.rs),
# so that COPY is a build dependency of both, not packaging.
COPY proxy/src /usr/build/proxy/src
COPY --from=console-builder /build/console/dist /usr/build/proxy/console/dist
COPY --from=frontend-builder /app/server/webapp/dist ./webapp/dist

# `touch`: COPY keeps every file's own modification time and cargo decides by
# modification time. A source file last edited before layer 0 was built would
# look older than the empty library compiled there, and cargo would keep that.
RUN touch /usr/build/crates/queen-protocol/src/lib.rs \
        /usr/build/protocols/queen-kafka/src/lib.rs \
        /usr/build/connectors/queen-s3/src/lib.rs \
        /usr/build/connectors/queen-pg/src/lib.rs \
        /usr/build/proxy/src/lib.rs \
    && cargo build --release --lib

# Layer 2: the broker. Manifests + build script + version file (build.rs embeds
# server.json's version into the binary via env!("QUEEN_VERSION")), then source.
COPY server/Cargo.toml server/Cargo.lock server/server.json server/build.rs ./
COPY server/src ./src

# Build. target/ holds the two layers above; the binary is copied out and the
# folder removed, so that this layer, the one that changes with every commit, is
# the size of the binary in the layer cache and not of the broker's build.
RUN touch build.rs src/lib.rs src/main.rs \
    && cargo build --release \
    && cp target/release/queen /queen \
    && rm -rf target

# Verify
RUN test -f /queen && echo "Build successful"

# Stage 3: Build queenctl (Go operator CLI)
FROM golang:1.26-alpine AS cli-builder

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
            -X 'github.com/smartpricing/queen/clients/client-cli/v2/cmd.BuildVersion=${QUEENCTL_VERSION}' \
            -X 'github.com/smartpricing/queen/clients/client-cli/v2/cmd.BuildCommit=${QUEENCTL_COMMIT}' \
            -X 'github.com/smartpricing/queen/clients/client-cli/v2/cmd.BuildDate=docker'" \
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
