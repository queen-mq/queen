#!/bin/bash
# How many tenants OF ONE PLAN fit in a cell of N cores, inside the SLO?
#
# Runs on the LOADER. The cell must already be built at the target core budget
# by capacity.sh, which caps PG + both brokers + proxy + balancer under ONE
# systemd slice (per-container --cpus would give each of the five its own N).
#
# Plans, exactly as specified — the queue count is part of the application shape
# a tenant is entitled to run, not an implementation detail to fold away:
#
#   plan  queues  parts/queue  total parts   rate    rate/queue
#   free       2          100          200   5/s          2.5
#   dev       10          100        1,000  25/s          2.5
#   pro       20          500       10,000  50/s          2.5
#
# goload drives ONE queue per tenant, so Q queues per tenant means Q concurrent
# goload processes over the same tenant set with distinct queue names, each
# carrying rate/Q. -pop-partitions is set to the full per-queue partition count:
# the isolation campaign showed a visit width below the partition count starves
# the tail partitions and surfaces as GAPS, which reads as data loss rather than
# as the config error it is.
set -uo pipefail

CELL=10.114.0.2
PROXY="http://$CELL:6711"
PLAN=${PLAN:?PLAN=free|dev|pro}
COUNTS=${COUNTS:?COUNTS="10 25 50"}
OUT=${OUT:-/root/cap-$PLAN}
DUR=${DUR:-120}
DRAIN=${DRAIN:-45}
mkdir -p "$OUT"

case "$PLAN" in
  free) QUEUES=2;  PARTS=100; RATE_Q=2.5; FILE=/root/c-free.json ;;
  dev)  QUEUES=10; PARTS=100; RATE_Q=2.5; FILE=/root/c-dev.json  ;;
  pro)  QUEUES=20; PARTS=500; RATE_Q=2.5; FILE=/root/c-pro.json  ;;
  *) echo "unknown plan $PLAN" >&2; exit 2 ;;
esac
# RATE_Q may be overridden to hold TOTAL rate constant while changing shape —
# e.g. pro at 250 msg/s/tenant to match dev x 100's 2500 msg/s over the same
# 100,000 partitions, isolating queue geometry from both rate and cardinality.
RATE_Q=${RATE_Q_OVERRIDE:-$RATE_Q}

for N in $COUNTS; do
  TOTPARTS=$(( N * QUEUES * PARTS ))
  TOTRATE=$(awk -v n=$N -v q=$QUEUES -v r=$RATE_Q 'BEGIN{printf "%.0f", n*q*r}')
  NAME="${PLAN}-n${N}"
  echo "################ $PLAN x $N tenants — ${QUEUES} queues x ${PARTS} parts = $(( QUEUES * PARTS ))/tenant, ${TOTPARTS} partitions total, ${TOTRATE} msg/s  $(date -u +%H:%M:%SZ)"

  ssh -o BatchMode=yes root@$CELL "rm -f /root/samples/$NAME.csv
    setsid nohup /root/sampler.sh cell-pg cell-broker-a cell-broker-b cell-proxy cell-lb \
      > /root/samples/$NAME.csv 2>/dev/null </dev/null & echo \$! > /root/cellsampler.pid" >/dev/null
  rm -f "$OUT/$NAME-loader.csv"
  setsid nohup /root/cmbench/sampler.sh > "$OUT/$NAME-loader.csv" 2>/dev/null </dev/null &
  echo $! > /root/loadsampler.pid

  PIDS=()
  for q in $(seq 1 $QUEUES); do
    /root/goload -mode cloud -target proxy -url "$PROXY" \
      -tenants-file "$FILE" -tenants "$N" \
      -queue "app-q$q" \
      -per-tenant-rate "$RATE_Q" -push-batch 1 \
      -partitions "$PARTS" -pop-partitions "$PARTS" \
      -active-fraction "${ACTIVE_FRACTION:-1.0}" \
      -consumers-per-tenant 1 -pop-wait -pop-timeout 5000 \
      -payload 256 -duration "$DUR" -drain "$DRAIN" -report 60 \
      -out "$OUT" -run-id "$NAME-q$q" > "$OUT/$NAME-q$q.log" 2>&1 &
    PIDS+=($!)
  done
  for p in "${PIDS[@]}"; do wait "$p" || true; done

  [ -f /root/loadsampler.pid ] && kill $(cat /root/loadsampler.pid) 2>/dev/null
  ssh -o BatchMode=yes root@$CELL '[ -f /root/cellsampler.pid ] && kill $(cat /root/cellsampler.pid) 2>/dev/null; true' >/dev/null
  scp -q root@$CELL:/root/samples/$NAME.csv "$OUT/$NAME-cell.csv" 2>/dev/null

  # Worst case across the Q queues is the tenant's experience: a tenant whose
  # queue 7 is slow does not care that queues 1-6 were fast.
  WORST=0; MISS=0; DUP=0; R429=0; CONF=0
  for q in $(seq 1 $QUEUES); do
    f="$OUT/$NAME-q$q.log"; [ -f "$f" ] || continue
    p99=$(grep -oE "p99= *[0-9.]+" "$f" | tail -1 | sed 's/.*p99= *//')
    [ -n "$p99" ] && WORST=$(awk -v a="$WORST" -v b="$p99" 'BEGIN{print (b>a)?b:a}')
    m=$(grep -E "^     TOTAL" "$f" | tail -1 | awk '{print $4+0}'); MISS=$((MISS + ${m:-0}))
    d=$(grep -E "^     TOTAL" "$f" | tail -1 | awk '{print $5+0}'); DUP=$((DUP + ${d:-0}))
    r=$(grep -oE "http_429:[0-9]+" "$f" | tail -1 | sed 's/.*://'); R429=$((R429 + ${r:-0}))
    grep -q "FAILED — a run over half-provisioned" "$f" && CONF=$((CONF+1))
  done
  VERD=$(awk -v w="$WORST" 'BEGIN{print (w>0 && w<=200)?"WITHIN SLO":"OVER SLO"}')
  echo "--- $PLAN n=$N: worst p99 across ${QUEUES} queues = ${WORST} ms -> $VERD | missing=$MISS dup=$DUP 429=$R429 configFail=$CONF"
done
echo "################ CAPACITY $PLAN DONE $(date -u +%H:%M:%SZ)"
