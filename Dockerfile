# Multi-stage Dockerfile for Queen Message Queue
#
# This Dockerfile builds the complete Queen Message Queue system:
# - C++ server with full C++17 support
# - Vue.js frontend dashboard
# - queenctl operator CLI (Go static binary)
# - Optimized runtime image (~250MB final size)
#
# Build: docker build -t queen-mq .
# Run:   docker run -p 6632:6632 -e PG_HOST=your-db queen-mq
#
# Operator CLI access from inside the container:
#   docker exec -it queen queenctl status      # zero-config: uses localhost:6632
#   docker exec -it queen queenctl tail orders --cg debug --follow
#
# For full stack with PostgreSQL, use docker-compose.yml
#
# Build optimizations:
# - Layered COPY: Makefile+deps cached separately from source code
# - Precompiled headers: spdlog + json.hpp parsed once, not 30+ times
# - ccache: compiler cache persisted across builds via BuildKit mount
# - Auto parallelism: uses $(nproc) instead of hardcoded -j value
# - queenctl built CGO-free against the local client-go via go.mod replace,
#   so a single static binary lands in the final image with no runtime deps.
#
# Pass --build-arg QUEENCTL_VERSION=$(jq -r .version server/server.json)
# to embed the same version string the broker reports. Defaults to "dev"
# when omitted.
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

# Build frontend
RUN npm run build

# Stage 2: Build C++ Server
FROM ubuntu:24.04 AS cpp-builder

ENV DEBIAN_FRONTEND=noninteractive
RUN sed -i -e 's|security.ubuntu.com|mirrors.edge.kernel.org|g' -e 's|archive.ubuntu.com|mirrors.edge.kernel.org|g' /etc/apt/sources.list.d/ubuntu.sources || true \
    && apt-get update && apt-get install -y \
    build-essential libpq-dev libssl-dev zlib1g-dev \
    curl unzip ca-certificates cmake ccache \
    && rm -rf /var/lib/apt/lists/*

# Enable ccache - wraps g++/gcc transparently for compilation caching
ENV PATH="/usr/lib/ccache:${PATH}"

WORKDIR /usr/build/server

# Layer 1: Copy only the Makefile (changes rarely - only when dep URLs change)
# This layer caches the dependency download so source code changes don't re-download
COPY server/Makefile ./Makefile

# Download and build dependencies (cached unless Makefile changes)
RUN make deps

# Layer 2: Copy source code (changes frequently, only triggers recompile)
COPY server/src/ ./src/
COPY server/include/ ./include/
COPY lib/ /usr/build/lib/

# Build with ccache persistent cache and auto-detected parallelism
RUN --mount=type=cache,target=/root/.ccache \
    make -j$(nproc) build-only

# Verify
RUN test -f bin/queen-server && echo "Build successful"

# Stage 3: Build queenctl (Go operator CLI)
FROM golang:1.24-alpine AS cli-builder

# Embed broker version + commit + build date into the binary so
# `queenctl version` reports the same string the broker does.
ARG QUEENCTL_VERSION=dev
ARG QUEENCTL_COMMIT=none

WORKDIR /src

# Copy only the two Go modules queenctl needs. client-go's path is
# referenced via the replace directive in client-cli/go.mod.
COPY clients/client-go/ ./clients/client-go/
COPY clients/client-cli/ ./clients/client-cli/

WORKDIR /src/clients/client-cli

# Pure-Go static build, no CGO so the binary runs on the ubuntu:24.04
# runtime stage (and on scratch images for that matter).
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOFLAGS=-trimpath go build \
        -ldflags "-s -w \
            -X 'github.com/smartpricing/queen/client-cli/cmd.BuildVersion=${QUEENCTL_VERSION}' \
            -X 'github.com/smartpricing/queen/client-cli/cmd.BuildCommit=${QUEENCTL_COMMIT}' \
            -X 'github.com/smartpricing/queen/client-cli/cmd.BuildDate=docker'" \
        -o /out/queenctl .

# Sanity check: the binary must run without any dynamic deps.
RUN /out/queenctl version --short

# Stage 4: Runtime Image
FROM ubuntu:24.04

# Install runtime dependencies + PostgreSQL 18 client tools (pg_dump, pg_restore)
# The PGDG repo is required because Ubuntu 24.04 only ships up to PG 16.
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

# Copy C++ server binary from builder
COPY --from=cpp-builder /usr/build/server/bin/queen-server ./bin/queen-server

# Copy schema and procedures for database initialization (from libqueen)
COPY --from=cpp-builder /usr/build/lib/schema ./schema

# Copy frontend build from builder
COPY --from=frontend-builder /app/webapp/dist ./webapp/dist

# Copy the queenctl operator CLI onto $PATH. With QUEEN_SERVER pre-set
# below, an in-container invocation needs no flags:
#   docker exec -it queen queenctl status
COPY --from=cli-builder /out/queenctl /usr/local/bin/queenctl

# In-container default for queenctl. Overridden by an explicit --server
# flag or by setting QUEEN_SERVER at `docker run -e ...` time.
ENV QUEEN_SERVER=http://localhost:6632

# Expose the server port
EXPOSE 6632

# Run the C++ server
CMD ["./bin/queen-server"]
