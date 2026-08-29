#!/bin/bash
# One directional cell on the native Mac rig: fresh database, fresh tenants,
# 60 s of load, one line of verdict.
#
#   ./local-run.sh <plan> <tenants> <active-fraction> [label]
#
# Every run DROPS and recreates the broker database and restarts the broker, so
# no run inherits another's partitions — the VM campaign showed 89k stale
# partitions poisoning configure with timeouts and inventing 548k spurious 429s.
#
# Directional only. The Mac's fdatasync is 18 us against the VM's 90, macOS has
# no cgroups to cap the cell, and the loader shares the machine. Shapes and
# ratios transfer; absolute capacity does not.
set -uo pipefail
export LANG=C LC_ALL=C

ROOT=/private/tmp/claude-502/-Users-alice-Work-queen/c440cf99-b13b-4314-af90-4276616ebefd/scratchpad/localcell
PGBIN=/opt/homebrew/opt/postgresql@18/bin
PGPORT=5470; BROKER_PORT=6640; PROXY_PORT=6741
export PGPASSWORD=$(cat "$ROOT/pgpass")
PLAN=${1:?plan}; N=${2:?tenants}; AF=${3:-1.0}; LABEL=${4:-$PLAN-n$N-af$AF}
DUR=${DUR:-60}; DRAIN=${DRAIN:-15}
OUT=$ROOT/runs/$LABEL; mkdir -p "$OUT"

case "$PLAN" in
  free) QUEUES=2;  PARTS=100; RATE_Q=2.5 ;;
  dev)  QUEUES=10; PARTS=100; RATE_Q=2.5 ;;
  pro)  QUEUES=20; PARTS=500; RATE_Q=2.5 ;;
  *) echo "unknown plan"; exit 2 ;;
esac
RATE_Q=${RATE_Q_OVERRIDE:-$RATE_Q}
# PARTS_OVERRIDE grows the DORMANT tail while active-fraction keeps the hot set
# fixed — the "10M entities, 1000 of them clicking right now" shape.
PARTS=${PARTS_OVERRIDE:-$PARTS}
# Pop width must be pinned INDEPENDENTLY of cardinality. Leaving it equal to the
# per-queue partition count made every high-cardinality run also a wide-pop run,
# which is why 5000 msg/s looked 400x better at 1M partitions than at 10k.
POP_PARTS=${POP_PARTS_OVERRIDE:-$PARTS}

# ---- fresh database + broker ------------------------------------------------
kill "$(cat "$ROOT/broker.pid" 2>/dev/null)" 2>/dev/null; sleep 1
"$PGBIN/psql" -h 127.0.0.1 -p $PGPORT -U postgres -qtAc "DROP DATABASE IF EXISTS queen WITH (FORCE)" >/dev/null
"$PGBIN/createdb" -h 127.0.0.1 -p $PGPORT -U postgres queen
"$PGBIN/psql" -h 127.0.0.1 -p $PGPORT -U postgres -d queen_proxy -qtAc \
  "DELETE FROM queen_proxy.clusters WHERE slug LIKE 'l%'" >/dev/null 2>&1
TOKIO_WORKER_THREADS=${CELL_THREADS:-4} PORT=$BROKER_PORT \
PG_HOST=127.0.0.1 PG_PORT=$PGPORT PG_USER=postgres PG_PASSWORD="$PGPASSWORD" PG_DATABASE=queen \
QUEEN_TENANCY_HEADER=true QUEEN_KV_TRUSTED_PROXY=1 LOG_LEVEL=info \
nohup /Users/alice/Work/queen/server/target/release/queen > "$OUT/broker.log" 2>&1 &
echo $! > "$ROOT/broker.pid"
for _ in $(seq 1 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$BROKER_PORT/health)" = "200" ] && break
  sleep 1
done

# ---- fresh tenants ----------------------------------------------------------
PFX="l${PLAN:0:1}$RANDOM"
rm -f "$OUT/tenants.json"
"$ROOT/goload" -mode provision -tenants "$N" -prefix "$PFX" -plan "$PLAN" -cell cell-local \
  -file "$OUT/tenants.json" \
  -psql-cmd "$PGBIN/psql -qtA -v ON_ERROR_STOP=1 -h 127.0.0.1 -p $PGPORT -U postgres -d queen_proxy" \
  > "$OUT/provision.log" 2>&1
grep -q "tenants in" "$OUT/provision.log" || { echo "PROVISION FAILED"; tail -2 "$OUT/provision.log"; exit 1; }

TOTPARTS=$(( N * QUEUES * PARTS ))
ACTIVE=$(awk -v p=$PARTS -v a=$AF 'BEGIN{v=int(p*a+0.999); print (v<1)?1:v}')
TOTRATE=$(awk -v n=$N -v q=$QUEUES -v r=$RATE_Q 'BEGIN{printf "%.0f", n*q*r}')

# ---- load -------------------------------------------------------------------
PIDS=()
for q in $(seq 1 $QUEUES); do
  "$ROOT/goload" -mode cloud -target proxy -url "http://127.0.0.1:$PROXY_PORT" \
    -tenants-file "$OUT/tenants.json" -tenants "$N" -queue "app-q$q" \
    -per-tenant-rate "$RATE_Q" -push-batch 1 \
    -partitions "$PARTS" -pop-partitions "$POP_PARTS" -active-fraction "$AF" \
    -consumers-per-tenant 1 -pop-wait -pop-timeout 5000 \
    -payload 256 -duration "$DUR" -drain "$DRAIN" -report 60 \
    -out "$OUT" -run-id "q$q" > "$OUT/q$q.log" 2>&1 &
  PIDS+=($!)
done
for p in "${PIDS[@]}"; do wait "$p" || true; done

# ---- verdict ----------------------------------------------------------------
WORST=0; MISS=0; DUP=0; R429=0; CONF=0
for q in $(seq 1 $QUEUES); do
  f="$OUT/q$q.log"; [ -f "$f" ] || continue
  p99=$(grep -oE "p99= *[0-9.]+" "$f" | tail -1 | sed 's/.*p99= *//')
  [ -n "$p99" ] && WORST=$(awk -v a="$WORST" -v b="$p99" 'BEGIN{print (b>a)?b:a}')
  # Default to 0: a run that produced no TOTAL line yields an empty expansion,
  # and $(( X +  )) is a bash syntax error that kills the whole matrix.
  m=$(grep -E "^     TOTAL" "$f" | tail -1 | awk '{print $4+0}'); MISS=$((MISS + ${m:-0}))
  d=$(grep -E "^     TOTAL" "$f" | tail -1 | awk '{print $5+0}'); DUP=$((DUP + ${d:-0}))
  r=$(grep -oE "http_429:[0-9]+" "$f" | tail -1 | sed 's/.*://'); R429=$((R429 + ${r:-0}))
  grep -q "FAILED — a run over half" "$f" && CONF=$((CONF+1))
done
V=$(awk -v w="$WORST" 'BEGIN{print (w>0 && w<=200)?"WITHIN":"OVER  "}')
printf '%-22s %-4s n=%-4s af=%-6s parts=%-9s %-6s msg/s  p99=%-9s %s  miss=%-5s dup=%-6s 429=%-5s cfgFail=%s\n' \
  "$LABEL" "$PLAN" "$N" "$AF" "$TOTPARTS" "$TOTRATE" "${WORST}ms" "$V" "$MISS" "$DUP" "$R429" "$CONF"
