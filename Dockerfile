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
FROM node:22-alpine AS frontend-builder

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

WORKDIR /usr/build/server

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
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/build/server/target \
    cargo build --release && cp target/release/queen-seg /queen-seg

# Verify
RUN test -f /queen-seg && echo "Build successful"

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
COPY --from=server-builder /queen-seg ./bin/queen-seg

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

# Run the Rust broker
CMD ["./bin/queen-seg"]
