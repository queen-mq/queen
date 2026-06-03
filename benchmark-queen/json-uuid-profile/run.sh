#!/usr/bin/env bash
#
# End-to-end measurement of the broker's JSON + UUID CPU overhead under hard
# push load.
#
# Flow:
#   1. up postgres + broker (profiling build), wait for /health
#   2. warmup load (fills caches, applies schema, creates partitions)
#   3. start measured load in background
#   4. after a short ramp: SIGUSR2 -> profiler ON
#   5. sample docker stats + bracket PG insert counters for the window
#   6. SIGUSR2 -> profiler OFF (flushes /profiles/queen.prof)
#   7. analyze.sh -> JSON/UUID/lock/alloc attribution
#
# All knobs are env-overridable; see docker-compose.yml. Results land in
# ./results/<timestamp>/.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

COMPOSE="docker compose"
BROKER_HOST_PORT="${BROKER_HOST_PORT:-6634}"
PROFILE_WINDOW="${PROFILE_WINDOW:-40}"
WARMUP_DURATION="${WARMUP_DURATION:-15}"
RAMP="${RAMP:-8}"
export DURATION="${DURATION:-60}"   # measured loadgen duration (>= RAMP + PROFILE_WINDOW)

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="results/$STAMP"
mkdir -p "$OUT_DIR" profiles
# gperftools signal-toggle mode writes queen.prof.0, .1, ... (one per on/off cycle).
rm -f profiles/queen.prof*

pg() { docker exec qjup-pg psql -U postgres -d queen -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }

echo "== [1/7] bring up postgres + broker =="
$COMPOSE up -d postgres broker

echo "== [2/7] wait for broker /health =="
healthy=0
for i in $(seq 1 120); do
  if curl -sf "http://localhost:${BROKER_HOST_PORT}/health" >/dev/null 2>&1; then healthy=1; break; fi
  sleep 1
done
if [[ "$healthy" != 1 ]]; then
  echo "ERROR: broker not healthy" >&2
  $COMPOSE logs --tail 60 broker >&2 || true
  exit 1
fi
echo "   broker healthy"

echo "== [3/7] warmup (${WARMUP_DURATION}s) =="
$COMPOSE run --rm -e WARMUP=1 -e DURATION="$WARMUP_DURATION" loadgen 2>&1 | sed 's/^/   /' || true

echo "== [4/7] start measured load (${DURATION}s, background) =="
$COMPOSE run --rm loadgen > "$OUT_DIR/loadgen.txt" 2>&1 &
LOAD_PID=$!

echo "   ramp ${RAMP}s before profiling..."
sleep "$RAMP"

# Bracket PG insert ground-truth around the profiled window.
INS_PRE="$(pg "select coalesce(n_tup_ins,0) from pg_stat_user_tables where schemaname='queen' and relname='messages'")"
T_PRE="$(date +%s.%N)"

echo "== [5/7] PROFILER ON =="
docker kill --signal=SIGUSR2 qjup-broker >/dev/null

# Sample container CPU/mem every 2s for the duration of the window.
( for _ in $(seq 1 $((PROFILE_WINDOW/2))); do
    docker stats --no-stream --format '{{.Name}}: cpu={{.CPUPerc}} mem={{.MemUsage}}' qjup-broker qjup-pg 2>/dev/null
    echo "---"
    sleep 2
  done ) > "$OUT_DIR/docker-stats.txt" 2>&1 &
STATS_PID=$!

sleep "$PROFILE_WINDOW"

echo "== [6/7] PROFILER OFF (flush) =="
docker kill --signal=SIGUSR2 qjup-broker >/dev/null
wait "$STATS_PID" 2>/dev/null || true

INS_POST="$(pg "select coalesce(n_tup_ins,0) from pg_stat_user_tables where schemaname='queen' and relname='messages'")"
T_POST="$(date +%s.%N)"

# Compute push msg/s ground truth (PG inserts into queen.messages).
MSG_PER_S="$(awk -v a="$INS_PRE" -v b="$INS_POST" -v t0="$T_PRE" -v t1="$T_POST" 'BEGIN{ d=b-a; dt=t1-t0; if(dt>0) printf "%.0f", d/dt; else print "0" }')"

# Wait for the profile to materialize on the host bind-mount, then resolve the
# newest queen.prof.<cycle> file (signal-toggle mode adds a numeric suffix).
PROF_HOST=""
for _ in $(seq 1 40); do
  PROF_HOST="$(ls -1t profiles/queen.prof* 2>/dev/null | head -n1)"
  [[ -n "$PROF_HOST" && -s "$PROF_HOST" ]] && break
  sleep 0.5
done
PROF_IN_CONTAINER="/profiles/$(basename "${PROF_HOST:-queen.prof.0}")"
cp -f "$PROF_HOST" "$OUT_DIR/$(basename "$PROF_HOST")" 2>/dev/null || true

echo "   waiting for loadgen to finish..."
wait "$LOAD_PID" 2>/dev/null || true

{
  echo "stamp=$STAMP"
  echo "profile_window_s=$PROFILE_WINDOW"
  echo "pg_messages_inserted_in_window=$((INS_POST - INS_PRE))"
  echo "push_msg_per_s_ground_truth=$MSG_PER_S"
  echo "broker_cpus=${BROKER_CPUS:-3.0} num_workers=${NUM_WORKERS:-3}"
  echo "load: conns=${CONNECTIONS:-200} workers=${LOADGEN_WORKERS:-4} batch=${PUSH_BATCH:-10} parts=${MAX_PARTITIONS:-200} payload=${PAYLOAD_SIZE_BYTES:-256}B"
} | tee "$OUT_DIR/run-meta.txt"

echo
echo "== [7/7] analyze =="
BROKER_CONTAINER=qjup-broker PROFILE="$PROF_IN_CONTAINER" COLLAPSED_OUT="" ./analyze.sh | tee "$OUT_DIR/analysis.txt"

echo
echo "Artifacts in $OUT_DIR/ :"
echo "  - analysis.txt     bucketed JSON/UUID/lock/alloc CPU attribution"
echo "  - queen.prof       raw gperftools profile (re-analyze with ./analyze.sh)"
echo "  - loadgen.txt      autocannon throughput/latency"
echo "  - docker-stats.txt broker/pg CPU during the window"
echo "  - run-meta.txt     PG-ground-truth msg/s + config"
echo
echo "Tip: stop the stack with:  docker compose down -v"
