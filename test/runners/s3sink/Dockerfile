# The S3 sink end-to-end suite (PLAN_S3_SINK.md §9). Build context = repo root.
#   docker build -f test/runners/s3sink/Dockerfile -t queen-test-runner-s3sink .
#
# The runner is DELIBERATELY self-contained: the sink binary, an S3 gateway and
# a lake reader all live in this image, and the compose stack stays exactly the
# one every other suite uses (pg + broker + runner). The alternative — adding a
# gateway service and a sink service to docker-compose.single.yml — would put
# two more containers in the path of every suite that does not need them, and
# would make the sink's own lifecycle (kill it at a crash point, start it again)
# a compose concern instead of a test step.
#
# Three things come from three places:
#   * queen-s3            built here from connectors/queen-s3 (release: the
#                         suite pushes six figures of records);
#   * versitygw           copied from the upstream image (Apache-2.0; MinIO is
#                         dead). Its posix backend makes the bucket a directory,
#                         so the reader can read the objects as FILES — no S3
#                         client in the assertion path at all;
#   * duckdb              the lake reader of plan §7, the same one the docs tell
#                         a user to point at the bucket.
#
# bookworm on both ends on purpose: a binary built against a NEWER glibc than
# the image that runs it fails at exec with a version error, and the failure
# looks like a missing binary rather than a toolchain mismatch.

# --- the sink -----------------------------------------------------------------
FROM rust:1-bookworm AS build
WORKDIR /src
# The crate has no path dependencies outside its own directory (the Dockerfile
# stage rule of the plan, §3), so this is the whole context it needs.
COPY connectors/queen-s3 ./connectors/queen-s3
WORKDIR /src/connectors/queen-s3
RUN cargo build --release --locked --bin queen-s3

# --- the gateway --------------------------------------------------------------
FROM versity/versitygw:latest AS gateway

# --- the runner ---------------------------------------------------------------
FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl jq zstd ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir duckdb

COPY --from=build   /src/connectors/queen-s3/target/release/queen-s3 /usr/local/bin/queen-s3
COPY --from=gateway /usr/local/bin/versitygw                         /usr/local/bin/versitygw

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/s3sink/scenarios.py       /suite/scenarios.py
COPY test/runners/s3sink/entrypoint.sh      /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /usr/local/bin/queen-s3 \
             /usr/local/bin/versitygw /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
