#!/usr/bin/env bash
# ============================================================================
# compare.sh — like-for-like throughput comparison: C++ Queen broker vs the
# Go hot-path spike, against the SAME Postgres, with the SAME stored procedures
# and the SAME goload loader. Runs the two brokers SEQUENTIALLY (never at the
# same time) so Postgres is never contended between them.
#
# This reproduces the local smoke comparison and is meant to be re-run on the
# real 32-core benchmark box for meaningful absolute numbers (a laptop with
# Postgres-in-Docker is PG-IO-bound and understates both brokers equally).
#
# Requirements: docker, go, and a built goload (path via GOLOAD=).
# Usage:
#   ./compare.sh                       # defaults below
#   DURATION=60 PARTITIONS=300 ./compare.sh
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")"
REPO_ROOT="$(cd .. && pwd)"

# ---- knobs -----------------------------------------------------------------
PG_PORT="${PG_PORT:-5544}"
CPP_TAG="${CPP_TAG:-smartnessai/queen-mq:0.16.0}"
CPP_PORT="${CPP_PORT:-6634}"
GO_PORT="${GO_PORT:-6633}"
DURATION="${DURATION:-30}"
PARTITIONS="${PARTITIONS:-100}"
PRODUCERS="${PRODUCERS:-100}"
CONSUMERS="${CONSUMERS:-60}"
PUSH_BATCH="${PUSH_BATCH:-10}"
POP_BATCH="${POP_BATCH:-200}"
POP_PARTITIONS="${POP_PARTITIONS:-5}"
PAYLOAD="${PAYLOAD:-256}"

# Benchmark tuning (identical for both brokers) — from the soak start-broker.sh
# / queen-inspect.json. See config.go / README.md.
export DB_POOL_SIZE="${DB_POOL_SIZE:-50}"
NUM_WORKERS="${NUM_WORKERS:-10}"
export QUEEN_PUSH_MAX_CONCURRENT="${QUEEN_PUSH_MAX_CONCURRENT:-24}"
export QUEEN_PUSH_MAX_HOLD_MS="${QUEEN_PUSH_MAX_HOLD_MS:-20}"
export QUEEN_PUSH_PREFERRED_BATCH_SIZE="${QUEEN_PUSH_PREFERRED_BATCH_SIZE:-50}"
export QUEEN_PUSH_MAX_BATCH_SIZE="${QUEEN_PUSH_MAX_BATCH_SIZE:-500}"
export QUEEN_POP_MAX_CONCURRENT="${QUEEN_POP_MAX_CONCURRENT:-16}"

GOLOAD="${GOLOAD:-/tmp/goload}"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }

# ---- 1. fresh Postgres + schema/procedures ---------------------------------
setup_pg() {
  log "starting qspike-pg on :$PG_PORT"
  docker rm -f qspike-pg >/dev/null 2>&1 || true
  docker run -d --name qspike-pg -e POSTGRES_PASSWORD=postgres -p "$PG_PORT":5432 \
    --shm-size=1g postgres:16 \
    -c max_connections=400 -c shared_buffers=4GB -c synchronous_commit=on \
    -c max_wal_size=8GB >/dev/null
  for i in $(seq 1 60); do docker exec qspike-pg pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
  log "loading schema + procedures"
  docker cp "$REPO_ROOT/lib/schema/schema.sql" qspike-pg:/tmp/schema.sql
  docker exec -i qspike-pg psql -U postgres -q -f /tmp/schema.sql >/dev/null
  for f in $(ls "$REPO_ROOT"/lib/schema/procedures/*.sql | sort); do
    docker cp "$f" qspike-pg:/tmp/proc.sql >/dev/null
    docker exec -i qspike-pg psql -U postgres -q -f /tmp/proc.sql >/dev/null 2>&1 || true
  done
}

# ---- 2. build goload if needed ---------------------------------------------
ensure_goload() {
  if [[ ! -x "$GOLOAD" ]]; then
    log "building goload"
    ( cd "$REPO_ROOT/benchmark-queen/2026-06-04/goload" && GOWORK=off go build -o "$GOLOAD" . )
  fi
}

pg_commits() {
  docker exec -i qspike-pg psql -U postgres -tAc \
    "select xact_commit from pg_stat_database where datname='postgres';" 2>/dev/null | tr -d ' ' || echo 0
}

# scrape_counter <broker-url> <metric-name> — reads a single value from the
# broker's Prometheus endpoint (C++ and Go expose the SAME series names).
scrape_counter() {
  curl -s "$1/metrics/prometheus" 2>/dev/null \
    | awk -v m="$2" '$1 ~ "^"m {print $2; exit}' || echo 0
}

# run_load runs goload while capturing broker req/msg counters and PG commits,
# then reports req/s, msg/s and fusion (messages per PG commit) for the run.
run_load() {
  local url="$1" queue="$2"
  local pr0 pm0 or0 om0 c0
  pr0=$(scrape_counter "$url" queen_cluster_push_requests_total)
  pm0=$(scrape_counter "$url" queen_cluster_push_messages_total)
  or0=$(scrape_counter "$url" queen_cluster_pop_requests_total)
  om0=$(scrape_counter "$url" queen_cluster_pop_messages_total)
  c0=$(pg_commits)

  "$GOLOAD" -mode max -url "$url" -queue "$queue" \
    -partitions "$PARTITIONS" -producers "$PRODUCERS" -consumers "$CONSUMERS" \
    -push-batch "$PUSH_BATCH" -pop-batch "$POP_BATCH" -pop-partitions "$POP_PARTITIONS" \
    -payload "$PAYLOAD" -duration "$DURATION" -report 5

  local pr1 pm1 or1 om1 c1
  pr1=$(scrape_counter "$url" queen_cluster_push_requests_total)
  pm1=$(scrape_counter "$url" queen_cluster_push_messages_total)
  or1=$(scrape_counter "$url" queen_cluster_pop_requests_total)
  om1=$(scrape_counter "$url" queen_cluster_pop_messages_total)
  c1=$(pg_commits)

  awk -v d="$DURATION" \
      -v pr=$((pr1-pr0)) -v pm=$((pm1-pm0)) -v or=$((or1-or0)) -v om=$((om1-om0)) -v c=$((c1-c0)) \
    'BEGIN{
      printf "  req/s:  push=%.0f  pop=%.0f\n", pr/d, or/d;
      printf "  msg/s:  push=%.0f  pop=%.0f\n", pm/d, om/d;
      printf "  PG commits/s=%.0f  fusion(push msgs/commit)=%.1f\n", c/d, (c>0? pm/c : 0);
    }'
}

# ---- 3. C++ broker ---------------------------------------------------------
bench_cpp() {
  log "=== C++ broker ($CPP_TAG) on :$CPP_PORT ==="
  docker rm -f qspike-cpp >/dev/null 2>&1 || true
  docker run -d --name qspike-cpp -p "$CPP_PORT":6632 \
    -e PG_HOST=host.docker.internal -e PG_PORT="$PG_PORT" -e PG_PASSWORD=postgres -e PG_USER=postgres \
    -e DB_POOL_SIZE="$DB_POOL_SIZE" -e NUM_WORKERS="$NUM_WORKERS" \
    -e QUEEN_PUSH_MAX_CONCURRENT -e QUEEN_PUSH_MAX_HOLD_MS -e QUEEN_PUSH_PREFERRED_BATCH_SIZE -e QUEEN_PUSH_MAX_BATCH_SIZE \
    -e QUEEN_POP_MAX_CONCURRENT -e QUEEN_CONCURRENCY_MODE=static \
    "$CPP_TAG" >/dev/null
  for i in $(seq 1 60); do curl -sf "http://localhost:$CPP_PORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
  ( sleep $((DURATION/2)); docker stats --no-stream --format '  {{.Name}}: cpu={{.CPUPerc}} mem={{.MemUsage}}' qspike-cpp qspike-pg ) &
  run_load "http://localhost:$CPP_PORT" cmp_cpp
  wait
  docker rm -f qspike-cpp >/dev/null 2>&1 || true
}

# ---- 4. Go broker ----------------------------------------------------------
bench_go() {
  log "=== Go hot-path broker on :$GO_PORT ==="
  GOWORK=off go build -o bin/go-hotpath ./...
  PG_HOST=localhost PG_PORT="$PG_PORT" PG_PASSWORD=postgres PORT="$GO_PORT" ./bin/go-hotpath >/tmp/go-hotpath.log 2>&1 &
  local gopid=$!
  for i in $(seq 1 60); do curl -sf "http://localhost:$GO_PORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
  ( sleep $((DURATION/2));
    echo "  go-hotpath: cpu%=$(ps -o %cpu= -p $gopid | tr -d ' ') mem=$(ps -o rss= -p $gopid | awk '{printf "%.1fMiB", $1/1024}')";
    docker stats --no-stream --format '  {{.Name}}: cpu={{.CPUPerc}} mem={{.MemUsage}}' qspike-pg ) &
  run_load "http://localhost:$GO_PORT" cmp_go
  wait
  kill "$gopid" >/dev/null 2>&1 || true
}

teardown() {
  log "teardown"
  docker rm -f qspike-cpp qspike-pg >/dev/null 2>&1 || true
  pkill -f 'bin/go-hotpath' >/dev/null 2>&1 || true
}

main() {
  ensure_goload
  setup_pg
  bench_cpp
  sleep 3
  bench_go
  echo
  log "done. Re-run on the 32-core benchmark box for meaningful ABSOLUTE numbers."
  log "(to clean up: docker rm -f qspike-cpp qspike-pg; pkill -f bin/go-hotpath)"
}

case "${1:-}" in
  teardown) teardown ;;
  *) main ;;
esac
