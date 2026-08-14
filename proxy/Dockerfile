# Multi-stage Dockerfile for the Queen proxy (gateway + operator console)
#
# IMPORTANT: the build context is the REPO ROOT, not proxy/.
#   Build: DOCKER_BUILDKIT=1 docker build -f proxy/Dockerfile -t queen-proxy .
#   Run:   docker run -p 6711:6711 -e PXDB_HOST=... -e PXDB_PASSWORD=... queen-proxy
#
# The context cannot be proxy/ because the crate embeds TWO Vite builds,
# one of which lives outside its own directory:
#
#   src/webapp.rs   #[folder = "../server/webapp/dist"]   <- app/, the broker's dashboard
#   src/console.rs  #[folder = "console/dist"]            <- proxy/console/
#
# rust-embed resolves both relative to the crate manifest dir and hard-errors at
# compile time when a folder is missing, so the two frontend stages below are
# build dependencies, not packaging steps. The layout under /usr/build mirrors
# the repo layout for exactly that reason — see the comment on stage 3.
#
# Unlike the broker image this one carries no PostgreSQL client tools and no
# libssl: the crate is rustls-only and cmake-free by design (Cargo.toml), and
# the proxy talks to PG through tokio-postgres-rustls. Nothing here shells out.

# Stage 1: Build the broker dashboard (app/)
FROM node:22-alpine AS webapp-builder

WORKDIR /app/webapp

COPY app/package*.json ./
RUN npm ci

COPY app/ ./

# vite writes to ../server/webapp/dist relative to the app source root
# (app/vite.config.js), which with this WORKDIR lands at /app/server/webapp/dist.
# Same artifact the broker image builds — the proxy serves it on the public leg.
RUN npm run build

# Stage 2: Build the operator console (proxy/console/)
FROM node:22-alpine AS console-builder

WORKDIR /build/console

COPY proxy/console/package*.json ./
RUN npm ci

COPY proxy/console/ ./

# outDir 'dist' with base '/console/' (console/vite.config.js) -> /build/console/dist.
RUN npm run build

# Stage 3: Build the Rust proxy
FROM rust:1-bookworm AS proxy-builder

# The crate sits at /usr/build/proxy so that its `../server/webapp/dist`
# embed resolves to /usr/build/server/webapp/dist. Moving either path breaks the
# build at compile time rather than at runtime, which is the point.
WORKDIR /usr/build/proxy

# Layer 1: manifests, so dependency compilation caches independently of src.
COPY proxy/Cargo.toml proxy/Cargo.lock ./

# Layer 2: source + migrations (embedded with include_str! from src/db.rs, so
# they are compiled into the binary — there is nothing to copy at runtime).
COPY proxy/src ./src
COPY proxy/migrations ./migrations

# Layer 3: the two embedded frontends, in the positions rust-embed expects.
COPY --from=console-builder /build/console/dist ./console/dist
COPY --from=webapp-builder /app/server/webapp/dist /usr/build/server/webapp/dist

# Build. Cargo registry + target dirs are BuildKit caches; copy the binary out of
# the (non-persisted) target cache so it lands in the image layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/build/proxy/target \
    cargo build --release && cp target/release/queen-proxy /queen-proxy

RUN test -f /queen-proxy && echo "Build successful"

# Stage 4: Runtime
#
# bookworm-slim, not ubuntu:24.04: the binary is linked against the builder's
# glibc 2.36, and matching the base exactly avoids betting on forward compat.
FROM debian:bookworm-slim

# ca-certificates is belt-and-braces — webpki-roots compiles the CA set into the
# binary — but it keeps the image useful for anything an operator runs in an
# exec shell. No libssl: nothing in the tree links OpenSSL.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Proxy binary. Console SPA, broker dashboard and SQL migrations are all
# compiled in, so this single file is the whole payload.
COPY --from=proxy-builder /queen-proxy ./bin/queen-proxy

# Default listener (QUEEN_PROXY_PORT, src/config.rs). The port is configurable;
# this EXPOSE documents the default rather than pinning it.
EXPOSE 6711

# Run as a non-root user. Plain useradd, not --system: the uid is deliberately
# above SYS_UID_MAX so it satisfies a runAsNonRoot admission policy, and
# --system would warn about that.
#
# The spool dir has to be created AND chowned here. It is the meter's durability
# fallback: when the proxy cannot reach its DB it writes usage records to disk
# instead of dropping them. An unwritable path only produces a WARN at startup
# and then silently loses billing data, so the image must not leave it to luck.
# Mount a volume over /app/spool to keep it across restarts.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin queen \
    && mkdir -p /app/spool \
    && chown 10001:10001 /app/spool
ENV QUEEN_PROXY_SPOOL_DIR=/app/spool

USER 10001

CMD ["./bin/queen-proxy"]
