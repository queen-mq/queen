# Rust broker unit tests. Build context = REPO ROOT.
#   docker build -f test/runners/rust/Dockerfile -t queen-test-runner-rust .
#
# The broker crate takes three crates by relative path — queen-protocol
# (../crates), the embedded proxy (../proxy) and the Kafka facade
# (../protocols/queen-kafka), the last two through its default features — and
# cargo resolves the whole dependency graph before it compiles anything, so all
# of them have to be in the context. Dockerfile.dockerignore keeps the root
# context light (buildx honors it).
#
# The #[test]/#[tokio::test] cases are in-process: the state machine, codecs,
# encryption, frames, the queen-protocol conformance suite — no broker process
# and no network. So this runner needs no stack; run.sh runs it standalone.
FROM rust:1-bookworm

WORKDIR /src
COPY crates ./crates
COPY protocols/queen-kafka/Cargo.toml ./protocols/queen-kafka/Cargo.toml
COPY protocols/queen-kafka/src ./protocols/queen-kafka/src
COPY proxy/Cargo.toml ./proxy/Cargo.toml
COPY proxy/src ./proxy/src
# The proxy embeds its console's committed build (`console/dist`) with
# rust_embed, which hard-errors at compile time when the folder is missing.
COPY proxy/console/dist ./proxy/console/dist
COPY server ./server

WORKDIR /src/server

# The broker embeds `webapp/dist` the same way. It is committed too (and
# copied with server/), but these tests never serve the dashboard, so an empty
# folder is enough if a checkout lacks it.
RUN mkdir -p webapp/dist

# Compile the test binaries at build time; ENTRYPOINT just runs them.
RUN cargo test --locked --no-run

ENTRYPOINT ["cargo", "test", "--locked"]
