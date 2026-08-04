# Rust broker unit tests. Build context = REPO ROOT.
#   docker build -f test/runners/rust/Dockerfile -t queen-test-runner-rust .
#
# The context used to be server/, which was enough while the crate had no path
# dependencies. server/Cargo.toml now takes queen-protocol as a DEV dependency
# at ../crates/queen-protocol, and cargo resolves the whole dependency graph —
# dev dependencies included — before it compiles anything, so the crate has to
# be in the context even though the release image never links it.
# Dockerfile.dockerignore keeps the root context light (buildx honors it).
#
# The #[test]/#[tokio::test] cases are pure in-process logic (mesh framing,
# dedup hashing, fusion, admission, encryption, frames, plus the queen-protocol
# conformance suite) — no broker and no Postgres. So this runner needs no
# stack; run.sh runs it standalone.
FROM rust:1-bookworm

WORKDIR /src
COPY crates ./crates
COPY server ./server

WORKDIR /src/server

# handlers/static_files.rs embeds `webapp/dist` with rust_embed, which
# hard-errors at compile time when the folder is missing. These tests never
# serve the dashboard, so an empty folder satisfies the macro and keeps the
# built frontend out of the test context entirely.
RUN mkdir -p webapp/dist

# Compile the test binaries at build time; ENTRYPOINT just runs them.
RUN cargo test --no-run

ENTRYPOINT ["cargo", "test"]
