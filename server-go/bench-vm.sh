#!/usr/bin/env bash
# ============================================================================
# bench-vm.sh — single-box, co-located benchmark: C++ Queen broker vs the Go
# hot-path spike, against the SAME Postgres, SAME stored procedures, SAME
# goload loader, and IDENTICAL tuning. Brokers run SEQUENTIALLY (never
# concurrently) so Postgres is never contended between them.
#
# Everything (PG + broker + loader) is co-located on this VM, so absolute
# throughput is somewhat loader/CPU-shared-limited (see the benchmark
# RESULTS.md note) — but the C++-vs-Go RELATIVE comparison is valid because the
# conditions are identical for both.
#
# Layout: PG + both brokers are containers on a docker network `qbench`
# (broker -> PG via PG_HOST=qbench-pg). goload runs natively on the host and
# hits the broker on localhost.
#
# Usage (on the VM):
#   /root/queen/server-go/bench-vm.sh setup     # PG + schema + images (once)
#   /root/queen/server-go/bench-vm.sh run        # C++ then Go, with metrics
#   DURATION=60 PARTITIONS=300 ./bench-vm.sh run
#   ./bench-vm.sh teardown
# ============================================================================
set -uo pipefail
REPO_ROOT="${REPO_ROOT:-/root/queen}"
cd "$REPO_ROOT/server-go"

NET=qbench
PG=qbench-pg
CPP=qbench-cpp
GO=qbench-go
CPP_TAG="${CPP_TAG:-smartnessai/queen-mq:0.16.0}"
GO_IMG=go-hotpath-spike:latest
CPP_PORT="${CPP_PORT:-6634}"
GO_PORT="${GO_PORT:-6633}"

# ---- load knobs ----
DURATION="${DURATION:-30}"
PARTITIONS="${PARTITIONS:-100}"
PRODUCERS="${PRODUCERS:-200}"
CONSUMERS="${CONSUMERS:-120}"
PUSH_BATCH="${PUSH_BATCH:-10}"
POP_BATCH="${POP_BATCH:-200}"
POP_PARTITIONS="${POP_PARTITIONS:-5}"
PAYLOAD="${PAYLOAD:-256}"
POP_WAIT="${POP_WAIT:-true}"          # long-poll pop (the real benchmark workload)
POP_TIMEOUT="${POP_TIMEOUT:-2000}"

# ---- IDENTICAL tuning for both brokers (soak values) ----
DB_POOL_SIZE="${DB_POOL_SIZE:-50}"
NUM_WORKERS="${NUM_WORKERS:-10}"
PUSH_CONC="${QUEEN_PUSH_MAX_CONCURRENT:-24}"
PUSH_HOLD="${QUEEN_PUSH_MAX_HOLD_MS:-20}"
PUSH_PREF="${QUEEN_PUSH_PREFERRED_BATCH_SIZE:-50}"
PUSH_MAX="${QUEEN_PUSH_MAX_BATCH_SIZE:-500}"
POP_CONC="${QUEEN_POP_MAX_CONCURRENT:-16}"
POP_PREF="${QUEEN_POP_PREFERRED_BATCH_SIZE:-20}"
POP_HOLD="${QUEEN_POP_MAX_HOLD_MS:-5}"

GOLOAD=/root/goload

log(){ echo "[$(date -u +%FT%TZ)] $*"; }

broker_env_args() {
  # Shared tuning env, passed identically to both brokers (each ignores the
  # knobs it doesn't have).
  echo "-e PG_HOST=$PG -e PG_PASSWORD=postgres -e PG_USER=postgres \
    -e DB_POOL_SIZE=$DB_POOL_SIZE -e NUM_WORKERS=$NUM_WORKERS \
    -e QUEEN_PUSH_MAX_CONCURRENT=$PUSH_CONC -e QUEEN_PUSH_MAX_HOLD_MS=$PUSH_HOLD \
    -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=$PUSH_PREF -e QUEEN_PUSH_MAX_BATCH_SIZE=$PUSH_MAX \
    -e QUEEN_POP_MAX_CONCURRENT=$POP_CONC -e QUEEN_POP_PREFERRED_BATCH_SIZE=$POP_PREF \
    -e QUEEN_POP_MAX_HOLD_MS=$POP_HOLD -e QUEEN_CONCURRENCY_MODE=static"
}

setup() {
  log "docker network + Postgres (soak tuning)"
  docker network create "$NET" >/dev/null 2>&1 || true
  docker rm -f "$PG" >/dev/null 2>&1 || true
  docker run -d --name "$PG" --network "$NET" --ulimit nofile=65535:65535 --shm-size=1g \
    -e POSTGRES_PASSWORD=postgres postgres:16 \
    -c max_connections=800 -c shared_buffers=24GB -c effective_cache_size=48GB \
    -c maintenance_work_mem=2GB -c work_mem=32MB -c wal_buffers=128MB \
    -c min_wal_size=8GB -c max_wal_size=48GB -c checkpoint_timeout=15min \
    -c checkpoint_completion_target=0.9 -c synchronous_commit=on -c wal_compression=on \
    -c random_page_cost=1.1 -c effective_io_concurrency=200 \
    -c autovacuum_vacuum_scale_factor=0.05 -c autovacuum_vacuum_cost_delay=2ms >/dev/null
  for i in $(seq 1 60); do docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

  log "loading schema + procedures"
  docker cp "$REPO_ROOT/lib/schema/schema.sql" "$PG":/tmp/schema.sql
  docker exec -i "$PG" psql -U postgres -q -f /tmp/schema.sql >/dev/null 2>&1
  for f in $(ls "$REPO_ROOT"/lib/schema/procedures/*.sql | sort); do
    docker cp "$f" "$PG":/tmp/proc.sql >/dev/null
    docker exec -i "$PG" psql -U postgres -q -f /tmp/proc.sql >/dev/null 2>&1 || true
  done

  log "pulling C++ image $CPP_TAG"
  docker pull "$CPP_TAG" >/dev/null

  log "building Go image"
  docker build -q -t "$GO_IMG" . >/dev/null

  log "building goload (native loader)"
  ( cd "$REPO_ROOT/benchmark-queen/2026-06-04/goload" && GOWORK=off go build -o "$GOLOAD" . )
  log "setup done"
}

pg_commits(){ docker exec -i "$PG" psql -U postgres -tAc \
  "select xact_commit from pg_stat_database where datname='postgres';" 2>/dev/null | tr -d ' ' || echo 0; }
scrape(){ curl -s "http://localhost:$1/metrics/prometheus" 2>/dev/null \
  | awk -v m="$2" '$1 ~ "^"m {print $2; exit}' || echo 0; }

run_load() {
  local port="$1" queue="$2" name="$3"
  local pr0 pm0 or0 om0 c0
  pr0=$(scrape "$port" queen_cluster_push_requests_total); pm0=$(scrape "$port" queen_cluster_push_messages_total)
  or0=$(scrape "$port" queen_cluster_pop_requests_total);  om0=$(scrape "$port" queen_cluster_pop_messages_total)
  c0=$(pg_commits)
  ( sleep $((DURATION/2)); log "docker stats mid-run:";
    docker stats --no-stream --format '    {{.Name}}: cpu={{.CPUPerc}} mem={{.MemUsage}}' "$name" "$PG" ) &
  "$GOLOAD" -mode max -url "http://localhost:$port" -queue "$queue" \
    -partitions "$PARTITIONS" -producers "$PRODUCERS" -consumers "$CONSUMERS" \
    -push-batch "$PUSH_BATCH" -pop-batch "$POP_BATCH" -pop-partitions "$POP_PARTITIONS" \
    -pop-wait="$POP_WAIT" -pop-timeout="$POP_TIMEOUT" \
    -payload "$PAYLOAD" -duration "$DURATION" -report 5
  wait
  local pr1 pm1 or1 om1 c1
  pr1=$(scrape "$port" queen_cluster_push_requests_total); pm1=$(scrape "$port" queen_cluster_push_messages_total)
  or1=$(scrape "$port" queen_cluster_pop_requests_total);  om1=$(scrape "$port" queen_cluster_pop_messages_total)
  c1=$(pg_commits)
  echo "  --- $name derived ---"
  awk -v d="$DURATION" -v pushr=$((pr1-pr0)) -v pushm=$((pm1-pm0)) -v popr=$((or1-or0)) -v popm=$((om1-om0)) -v cmt=$((c1-c0)) \
    'BEGIN{ printf "  req/s: push=%.0f pop=%.0f | msg/s: push=%.0f pop=%.0f | commits/s=%.0f fusion(pushmsg/commit)=%.1f\n",
      pushr/d, popr/d, pushm/d, popm/d, cmt/d, (cmt>0?pushm/cmt:0) }'
}

bench_cpp() {
  log "=== C++ broker ($CPP_TAG) :$CPP_PORT ==="
  docker rm -f "$CPP" >/dev/null 2>&1 || true
  docker run -d --name "$CPP" --network "$NET" --ulimit nofile=65535:65535 -p "$CPP_PORT":6632 \
    $(broker_env_args) "$CPP_TAG" >/dev/null
  for i in $(seq 1 90); do curl -sf "http://localhost:$CPP_PORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
  run_load "$CPP_PORT" cmp_cpp "$CPP"
  docker rm -f "$CPP" >/dev/null 2>&1 || true
}

bench_go() {
  log "=== Go hot-path broker :$GO_PORT ==="
  docker rm -f "$GO" >/dev/null 2>&1 || true
  docker run -d --name "$GO" --network "$NET" --ulimit nofile=65535:65535 -p "$GO_PORT":6632 \
    $(broker_env_args) "$GO_IMG" >/dev/null
  for i in $(seq 1 90); do curl -sf "http://localhost:$GO_PORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
  run_load "$GO_PORT" cmp_go "$GO"
  docker rm -f "$GO" >/dev/null 2>&1 || true
}

teardown(){ docker rm -f "$CPP" "$GO" "$PG" >/dev/null 2>&1 || true; docker network rm "$NET" >/dev/null 2>&1 || true; log "torn down"; }

case "${1:-run}" in
  setup) setup ;;
  run) bench_cpp; sleep 5; bench_go; log "done" ;;
  all) setup; bench_cpp; sleep 5; bench_go; log "done" ;;
  teardown) teardown ;;
  *) echo "usage: $0 {setup|run|all|teardown}"; exit 2 ;;
esac
