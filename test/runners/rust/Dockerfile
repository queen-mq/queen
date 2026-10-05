# Rust broker unit tests. Build context = REPO ROOT.
#   docker build -f test/runners/rust/Dockerfile -t queen-test-runner-rust .
#
# The broker crate takes four crates by relative path — queen-protocol
# (../crates), the embedded proxy (../proxy), the Kafka facade
# (../protocols/queen-kafka) and the Postgres connectors
# (../connectors/queen-pg), the last three through its default features — and
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
# The Postgres connectors (../connectors/queen-pg), through the default `pg`
# feature: the same rule, the whole graph resolves before anything compiles.
COPY connectors/queen-pg/Cargo.toml ./connectors/queen-pg/Cargo.toml
COPY connectors/queen-pg/src ./connectors/queen-pg/src
COPY connectors/queen-s3/Cargo.toml ./connectors/queen-s3/Cargo.toml
COPY connectors/queen-s3/src ./connectors/queen-s3/src
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
