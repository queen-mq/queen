#!/usr/bin/env bash
# Shared machinery for the two PLAN_KV_TIMERS performance gates. Sourced, never run.
#
# The whole file exists to make one number trustworthy: CPU microseconds per message.
# Everything else here is in service of that — cumulative counters instead of samples, a fixed
# amount of work instead of a fixed duration, a fresh database per capture, and a spread check
# that refuses to answer a 1% question with data that cannot resolve 1%.

set -uo pipefail

GATES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${GATE_RESULTS_DIR:-$GATES_DIR/results}"
COMPOSE_FILE="$GATES_DIR/compose.yml"
PROJECT="${GATE_PROJECT:-kvtgate}"
BROKER_PORT="${GATE_BROKER_PORT:-16644}"
BROKER_URL="http://localhost:$BROKER_PORT"

die()  { printf '\n\033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }
info() { printf '  %s\n' "$*"; }
step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

need() { command -v "$1" >/dev/null 2>&1 || die "$1 is required and not on PATH"; }

# --------------------------------------------------------------------------- stack

compose() { docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"; }

cid() { compose ps -q "$1" 2>/dev/null | head -1; }

stack_up() {
  # Always from zero. A capture that inherits the previous one's heap, bloat, cache state and
  # log_txns window is not comparable with anything, and the differences are the same order as
  # the effect being measured.
  compose down -v --remove-orphans >/dev/null 2>&1
  compose up -d >/dev/null 2>"$GATES_DIR/.compose.err" || {
    cat "$GATES_DIR/.compose.err" >&2
    die "docker compose up failed"
  }
  wait_broker
}

stack_down() { compose down -v --remove-orphans >/dev/null 2>&1; }

wait_broker() {
  local i
  for i in $(seq 1 120); do
    if curl -fsS "$BROKER_URL/health" >/dev/null 2>&1; then
      # The broker answers /health before its first schema apply has settled on a cold volume;
      # one more beat keeps DDL out of the measured window.
      sleep 2
      return 0
    fi
    sleep 1
  done
  compose logs --tail 40 broker >&2
  die "broker did not come up on $BROKER_URL within 120s"
}

psql_q() { docker exec -i "$(cid pg)" psql -U postgres -d postgres -Atq -F $'\t' -c "$1"; }
psql_f() { docker exec -i "$(cid pg)" psql -U postgres -d postgres -Atq -F $'\t'; }

# --------------------------------------------------------------------------- CPU

# Cumulative CPU microseconds of a container, read from its own cgroup.
#
# Cumulative, not sampled: `docker stats` reports a percentage over a window it chooses, and two
# percentages a minute apart cannot be subtracted into an amount of work. cpu.stat is a counter,
# so the delta across the measured window is exactly the CPU that window consumed, with no
# sampling error at all.
cpu_usec() {
  local c out
  c="$(cid "$1")" || true
  [ -n "$c" ] || die "container for service '$1' is not running"
  out="$(docker exec "$c" sh -c \
    'cat /sys/fs/cgroup/cpu.stat 2>/dev/null | awk "/^usage_usec/{print \$2}" \
     || awk "{print int(\$1/1000)}" /sys/fs/cgroup/cpuacct/cpuacct.usage' 2>/dev/null)"
  case "$out" in
    ''|*[!0-9]*) die "could not read cgroup CPU for '$1'. This gate needs cgroup v2 (usage_usec)
       or cgroup v1 (cpuacct.usage) inside the container; on Docker Desktop both live in the
       Linux VM and are readable. Without a CPU counter there is no gate — do not fall back to
       docker stats, its sampled percentages cannot resolve 1%." ;;
  esac
  echo "$out"
}

# --------------------------------------------------------------------------- pg_stat_statements

pgss_assert_available() {
  local pre
  pre="$(psql_q "SHOW shared_preload_libraries")"
  case "$pre" in
    *pg_stat_statements*) : ;;
    *) die "pg_stat_statements is not preloaded (shared_preload_libraries = '$pre').
       CREATE EXTENSION would succeed and record NOTHING, and 'no new query' would then be a
       lie rather than a result. Use this directory's compose.yml, which preloads it." ;;
  esac
  psql_q "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null
}

pgss_reset() { psql_q "/*gate*/ SELECT pg_stat_statements_reset()" >/dev/null; }

# Every normalized statement with calls > 0, minus this script's own probes and the extension's
# own bookkeeping. One statement per line: "<calls>\t<query on one line>".
#
# Compared by TEXT, never by queryid: queryid hashes relation OIDs, and every capture runs
# against a freshly created database, so the same statement legitimately has a different queryid
# in the before and after runs.
pgss_dump() {
  psql_q "/*gate*/ SELECT calls, regexp_replace(query, '\s+', ' ', 'g')
            FROM pg_stat_statements
           WHERE calls > 0
             AND query NOT LIKE '%/*gate*/%'
             AND query NOT LIKE '%pg_stat_statements%'
        ORDER BY 2"
}

# --------------------------------------------------------------------------- statistics

# median of stdin (one number per line)
median() { sort -n | awk '{v[NR]=$1} END{ if(NR==0){print 0; exit} m=int((NR+1)/2); print (NR%2)?v[m]:(v[m]+v[m+1])/2 }'; }
minv()   { sort -n | head -1; }
maxv()   { sort -n | tail -1; }

# percentage delta b vs a, signed, two decimals
pct() { awk -v a="$1" -v b="$2" 'BEGIN{ if(a==0){print "inf"; exit} printf "%.3f", (b-a)*100.0/a }'; }

# spread of a sample as a percentage of its median — the honesty check. A 1% gate read off
# repetitions that themselves scatter by 3% is not a measurement, it is a coin toss with a
# decimal point.
spread_pct() {
  local f="$1" med mn mx
  med="$(median <"$f")"; mn="$(minv <"$f")"; mx="$(maxv <"$f")"
  awk -v med="$med" -v mn="$mn" -v mx="$mx" 'BEGIN{ if(med==0){print "inf"; exit} printf "%.3f", (mx-mn)*100.0/med }'
}

# --------------------------------------------------------------------------- load

loadgen() { node "$GATES_DIR/loadgen.mjs" "$@"; }

# --------------------------------------------------------------------------- results

label_dir() { echo "$RESULTS_DIR/$1"; }

save_env() {
  local dir="$1"
  {
    echo "date=$(date -u +%FT%TZ)"
    echo "host=$(uname -srm)"
    echo "image=${GATE_IMAGE:-queen:test}"
    echo "image_id=$(docker image inspect -f '{{.Id}}' "${GATE_IMAGE:-queen:test}" 2>/dev/null)"
    echo "broker_cpus=${GATE_BROKER_CPUS:-4}"
    echo "pg_cpus=${GATE_PG_CPUS:-4}"
    echo "sync_commit=${GATE_SYNC_COMMIT:-on}"
    echo "pool_size=${GATE_POOL_SIZE:-32}"
    echo "kv_enabled=${GATE_KV_ENABLED:-false}"
    echo "timers_enabled=${GATE_TIMERS_ENABLED:-false}"
    echo "reps=${GATE_REPS:-3}"
    echo "bundles=${GATE_BUNDLES:-20000}"
    echo "items=${GATE_ITEMS:-20}"
    echo "workers=${GATE_WORKERS:-8}"
    echo "partitions=${GATE_PARTITIONS:-16}"
  } >"$dir/env.txt"
}
