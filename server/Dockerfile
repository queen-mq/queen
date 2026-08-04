# Slim broker image for the test matrix (`queen:test`).
#
# BUILD CONTEXT IS THE REPO ROOT, not ./server:
#
#   docker build -f server/Dockerfile -t queen:test .
#
# `server/Cargo.toml` takes `queen-protocol` by the relative path
# `../crates/queen-protocol`, so the crate has to be inside the context.
# Building from ./server leaves it outside and cargo stops with
# "failed to read /crates/queen-protocol/Cargo.toml" — which is exactly how
# this broke when the shared crate landed: the root Dockerfile was updated for
# it, this one was not.
FROM rust:1-bookworm AS build

# The crate first: it changes far less often than the broker's own sources, so
# it earns its own layer.
WORKDIR /src
COPY crates ./crates

WORKDIR /src/server
COPY server/Cargo.toml server/Cargo.lock server/build.rs server/server.json ./
COPY server/src ./src
COPY server/sql ./sql
COPY server/webapp ./webapp

# handlers/static_files.rs embeds `webapp/dist` with rust_embed, which
# hard-errors at compile time when the directory is missing. The committed dist
# is normally there; this keeps the build working if a .dockerignore rule ever
# takes it out of the context again.
RUN mkdir -p webapp/dist && cargo build --release

FROM debian:bookworm-slim
COPY --from=build /src/server/target/release/queen /queen
EXPOSE 6632
ENV PORT=6632
ENTRYPOINT ["/queen"]
