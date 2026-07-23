# Rust broker unit tests. Build context = server/ (so server/.dockerignore keeps
# the 3.6 GB target/ out).
#   docker build -f test/runners/rust/Dockerfile -t queen-test-runner-rust server
#
# The 50 #[test]/#[tokio::test] cases in the crate are pure in-process logic
# (mesh framing, dedup hashing, fusion, vegas, encryption, frames) — no broker
# and no Postgres. So this runner needs no stack; run.sh runs it standalone.
FROM rust:1-bookworm

WORKDIR /src
COPY . .

# Compile the test binaries at build time; ENTRYPOINT just runs them.
RUN cargo test --no-run

ENTRYPOINT ["cargo", "test"]
