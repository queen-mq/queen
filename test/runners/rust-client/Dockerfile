# Rust CLIENT integration suite. Build context = repo root.
#   docker build -f test/runners/rust-client/Dockerfile -t queen-test-runner-rust-client .
#
# Not to be confused with test/runners/rust, which runs the BROKER's in-process
# unit tests and needs no stack. This one drives clients/client-rust against a
# live broker, so run.sh treats it as a client suite (single / ha / tenanted).
FROM rust:1-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /suite

# The client takes queen-protocol by relative path (../../crates/...), so the
# layout under /suite has to mirror the repo.
COPY crates ./crates
COPY clients/client-rust ./clients/client-rust

WORKDIR /suite/clients/client-rust

# Compile the test binaries at build time so a slow first cargo build does not
# eat into the suite's wall clock (and cannot be mistaken for a hung broker).
RUN cargo test --locked --no-run

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/rust-client/entrypoint.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
