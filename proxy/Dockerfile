# syntax=docker/dockerfile:1

# Base images are build-args so operators can swap in their own hardened bases
# (e.g. Docker Hardened Images / Chainguard) without editing this file.
#
# IMPORTANT: builder and runtime must share libc. `bcrypt` is a native addon,
# so a binary built on alpine (musl) will not load on a Debian (glibc) runtime.
# Both defaults below are glibc + Node 22, keeping the compiled binary portable
# between the two stages.
ARG BUILDER_IMAGE=node:22-bookworm-slim
ARG RUNTIME_IMAGE=gcr.io/distroless/nodejs22-debian12:nonroot

# =============================================================================
# Stage 1 — build: install production dependencies.
# =============================================================================
FROM ${BUILDER_IMAGE} AS build
WORKDIR /app

# Toolchain for any native module (bcrypt) whose prebuilt binary is missing for
# the target arch. Lives only in this stage, so it never reaches the final
# image or its CVE surface.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 make g++ \
    && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm ci --omit=dev

# =============================================================================
# Stage 2 — runtime: distroless hardened base (issue #32).
#
# Why distroless instead of node:22(-alpine):
#   - No shell, package manager, or busybox → far smaller attack surface and
#     dramatically fewer CVEs than the default Node images, which ship a full
#     userland. This is the "hardened base image" the issue asked for.
#   - The `:nonroot` tag runs as uid 65532 — the proxy never runs as root.
#   - Pinned to Node 22 (same as the builder) for reproducible builds.
#
# Drop in a different hardened base without touching anything else, e.g.:
#   --build-arg RUNTIME_IMAGE=cgr.dev/chainguard/node:latest   # Chainguard
#   --build-arg RUNTIME_IMAGE=<your-org>/dhi-node:22           # Docker Hardened Images
# =============================================================================
FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /app
ENV NODE_ENV=production

COPY --from=build /app/node_modules ./node_modules
COPY src ./src
COPY public ./public

EXPOSE 3000

# Distroless ships no wget/curl/sh, so the healthcheck can't shell out. Use
# Node's built-in global fetch (Node 18+) instead. `node` lives at
# /nodejs/bin/node; HEALTHCHECK execs the command directly (it does not run
# through the image ENTRYPOINT), so the absolute path is required.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD ["/nodejs/bin/node", "-e", "fetch('http://127.0.0.1:3000/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"]

# The distroless nodejs image sets ENTRYPOINT ["/nodejs/bin/node"], so CMD only
# needs the script path.
CMD ["src/server.js"]
