#!/usr/bin/env bash
# Rust client integration suite.
set -eu

# The suite skips every integration test when this is unset, so it is what
# turns the run from "unit tests only" into a real one. Exported before the
# broker wait so a failure below is unambiguous.
export QUEEN_TEST_URL="$QUEEN_HTTP_URL"

# ...and this makes the skip branch itself fatal. Without it, an empty
# QUEEN_HTTP_URL would skip all ~130 integration tests and still exit 0, which
# reads exactly like a passing run.
export QUEEN_TEST_STRICT=1

/usr/local/bin/wait-for-broker

cd /suite/clients/client-rust

# One `cargo test` covers the lot: cargo runs the lib unit tests and each
# integration binary in turn, never two at once, which is what keeps the
# broker-global maintenance suite away from the others.
exec cargo test --locked
