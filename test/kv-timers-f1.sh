#!/usr/bin/env bash
#
# The three F1 rows of PLAN_KV_TIMERS.md §15, in one command.
#
#   1. Unit SQL of the pure helpers        -> server/tests/kv_sql_helpers.rs
#   2. Boot idempotence (x2 virgin, x1 populated)
#                                          -> server/tests/kv_timers_boot_idempotence.rs
#   3. The 42P22 verification on the rig   -> server/tests/kv_collation_42p22.rs
#
# These are `#[ignore]` cargo integration tests, the convention every DB-backed
# test in this repo already follows (embedded_smoke, hotlist_repairs,
# hotlist_reseed_window): a throwaway Postgres named by QUEEN_EMBEDDED_TEST_PG,
# and a real `Broker::start` to apply the schema. test/run.sh does NOT run them —
# its `rust` suite is the in-process unit tests, which need no stack — so this
# script is the entry point, and run.sh is left untouched.
#
# WHY Broker::start AND NOT psql. The SQL is include_str!-embedded in
# server/src/schema.rs. A psql script would test the file on disk and pass
# happily against a stale binary; booting the broker is what proves the .sql the
# binary carries is the .sql that was edited. Which is also why this script runs
# `cargo build` first, and says so.
#
# Usage:
#   test/kv-timers-f1.sh                 # own throwaway PG on port 5471
#   test/kv-timers-f1.sh --keep          # leave the container up afterwards
#   QUEEN_EMBEDDED_TEST_PG=host:port test/kv-timers-f1.sh   # use an existing PG
#
# NEVER point this at :5432 — that is the LIVE channel-ts stack.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER_DIR="$(cd "$SCRIPT_DIR/../server" && pwd)"

CONTAINER=queen-kvt-pg
PORT=5471
KEEP=0
OWN_PG=0

while [ $# -gt 0 ]; do
  case "$1" in
    --keep) KEEP=1; shift;;
    -h|--help) sed -n '2,32p' "$0"; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

if [ -z "${QUEEN_EMBEDDED_TEST_PG:-}" ]; then
  command -v docker >/dev/null || { echo "docker not found, and QUEEN_EMBEDDED_TEST_PG is unset" >&2; exit 2; }
  OWN_PG=1
  echo ">> starting throwaway Postgres on :$PORT"
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  docker run --rm -d --name "$CONTAINER" \
    -e POSTGRES_PASSWORD=postgres -p "$PORT:5432" postgres:16-alpine >/dev/null || exit 2
  for _ in $(seq 1 60); do
    docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
    sleep 0.5
  done
  export QUEEN_EMBEDDED_TEST_PG="localhost:$PORT"
fi

case "$QUEEN_EMBEDDED_TEST_PG" in
  *:5432) echo "REFUSING: :5432 is the LIVE channel-ts stack" >&2; exit 2;;
esac

echo ">> PG: $QUEEN_EMBEDDED_TEST_PG"

# The GOTCHA that costs half a day: the SQL is include_str!-embedded, so an edit
# to a .sql file has no effect until the crate is rebuilt. A test run against a
# stale binary does not fail, it LIES.
echo ">> cargo build (the SQL is include_str!-embedded; a stale binary lies)"
( cd "$SERVER_DIR" && cargo build --tests ) || { echo "build failed" >&2; exit 1; }

rc=0
for t in kv_sql_helpers kv_timers_boot_idempotence kv_collation_42p22; do
  echo
  echo "=============================== $t"
  ( cd "$SERVER_DIR" && cargo test --test "$t" -- --ignored --nocapture ) || rc=1
done

if [ "$OWN_PG" = 1 ] && [ "$KEEP" = 0 ]; then
  docker rm -f "$CONTAINER" >/dev/null 2>&1
else
  [ "$OWN_PG" = 1 ] && echo ">> container $CONTAINER left running (--keep)"
fi

echo
[ "$rc" = 0 ] && echo "F1 test rows: GREEN" || echo "F1 test rows: RED (expected until 024_kv.sql / 025_log_timers.sql exist)"
exit "$rc"
