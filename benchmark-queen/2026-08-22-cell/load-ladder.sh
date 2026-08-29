#!/bin/bash
# Contention ceiling: FIXED tenant count, RAMPED offered load.
#
# The tenant-count ladder answered "how many tenants fit at ~4.4% of their
# entitlement" — a number that quietly assumed the oversubscription ratio it
# was supposed to measure. This ladder fixes the population and moves the one
# variable that actually decides how far a plan can be oversold.
#
# Population: 5000 tenants, 2500 free / 1500 dev / 1000 pro, 10 partitions each.
# Base rates (mult=1.00): free 0.2, dev 2, pro 10 msg/s = 13,500 msg/s offered.
# Entitlement sold to that population: 2500x20 + 1500x40 + 1000x200 = 310,000/s.
# So mult=1.00 is 4.4% contention, and each rung reports the ratio it reached.
set -uo pipefail

CELL=10.114.0.2
PROXY="http://$CELL:6711"
OUT=${OUT:-/root/loadladder}
DUR=${DUR:-150}
DRAIN=${DRAIN:-50}
PARTS=${PARTS:-10}
NFREE=${NFREE:-2500}; NDEV=${NDEV:-1500}; NPRO=${NPRO:-1000}
MULTS=${MULTS:-"0.25 0.50 0.75 1.00"}
ENTITLED=$(awk -v f=$NFREE -v d=$NDEV -v p=$NPRO 'BEGIN{print f*20 + d*40 + p*200}')
mkdir -p "$OUT"

if [ "${WARMUP:-1}" = "1" ]; then
  echo "################ WARM-UP: $((NFREE+NDEV+NPRO)) tenants x $PARTS partitions $(date -u +%H:%M:%SZ)"
  WP=()
  for spec in "free:/root/t-free.json:$NFREE" "dev:/root/t-dev.json:$NDEV" "pro:/root/t-pro.json:$NPRO"; do
    IFS=: read -r wl wf wn <<<"$spec"
    /root/goload -mode cloud -target proxy -url "$PROXY" \
      -tenants-file "$wf" -tenants "$wn" -per-tenant-rate 0.3 -push-batch 1 \
      -partitions "$PARTS" -pop-partitions "$PARTS" \
      -consumers-per-tenant 1 -pop-wait -pop-timeout 3000 -payload 256 \
      -duration 40 -drain 25 -report 60 -verify=false \
      -out "$OUT" -run-id "warm-$wl" > "$OUT/warm-$wl.log" 2>&1 &
    WP+=($!)
  done
  for p in "${WP[@]}"; do wait "$p" || true; done
  grep -h "configure" "$OUT"/warm-*.log | tail -3
  echo "warm-up done $(date -u +%H:%M:%SZ)"
fi

for M in $MULTS; do
  NAME="L$(awk -v m=$M 'BEGIN{printf "%03d", m*100}')"
  RF=$(awk -v m=$M 'BEGIN{printf "%.3f", 0.2*m}')
  RD=$(awk -v m=$M 'BEGIN{printf "%.3f", 2*m}')
  RP=$(awk -v m=$M 'BEGIN{printf "%.3f", 10*m}')
  OFFERED=$(awk -v f=$NFREE -v d=$NDEV -v p=$NPRO -v rf=$RF -v rd=$RD -v rp=$RP 'BEGIN{printf "%.0f", f*rf + d*rd + p*rp}')
  PCT=$(awk -v o=$OFFERED -v e=$ENTITLED 'BEGIN{printf "%.2f", 100*o/e}')
  echo "################ $NAME — mult=$M  offered=${OFFERED} msg/s = ${PCT}% of the ${ENTITLED} msg/s sold  $(date -u +%H:%M:%SZ)"

  ssh -o BatchMode=yes root@$CELL "rm -f /root/samples/$NAME.csv
    setsid nohup /root/sampler.sh cell-pg cell-broker-a cell-broker-b cell-proxy cell-lb \
      > /root/samples/$NAME.csv 2>/dev/null </dev/null & echo \$! > /root/cellsampler.pid" >/dev/null
  rm -f "$OUT/$NAME-loader.csv"
  setsid nohup /root/cmbench/sampler.sh > "$OUT/$NAME-loader.csv" 2>/dev/null </dev/null &
  echo $! > /root/loadsampler.pid

  PIDS=()
  launch() { # <label> <file> <n> <rate> <batch>
    [ "$3" -eq 0 ] && return 0
    /root/goload -mode cloud -target proxy -url "$PROXY" \
      -tenants-file "$2" -tenants "$3" -per-tenant-rate "$4" -push-batch "$5" \
      -partitions "$PARTS" -pop-partitions "$PARTS" \
      -consumers-per-tenant 1 -pop-wait -pop-timeout 5000 \
      -payload 256 -duration "$DUR" -drain "$DRAIN" -report 60 \
      -out "$OUT" -run-id "$NAME-$1" > "$OUT/$NAME-$1.log" 2>&1 &
    PIDS+=($!)
  }
  launch free /root/t-free.json "$NFREE" "$RF" 1
  launch dev  /root/t-dev.json  "$NDEV"  "$RD" 1
  launch pro  /root/t-pro.json  "$NPRO"  "$RP" 2
  for p in "${PIDS[@]}"; do wait "$p" || true; done

  [ -f /root/loadsampler.pid ] && kill $(cat /root/loadsampler.pid) 2>/dev/null
  ssh -o BatchMode=yes root@$CELL '[ -f /root/cellsampler.pid ] && kill $(cat /root/cellsampler.pid) 2>/dev/null; true' >/dev/null
  scp -q root@$CELL:/root/samples/$NAME.csv "$OUT/$NAME-cell.csv" 2>/dev/null

  echo "--- $NAME (${PCT}% contention) ---"
  WORST=0
  for label in free dev pro; do
    f="$OUT/$NAME-$label.log"; [ -f "$f" ] || continue
    printf '  %-5s ' "$label"
    grep -oE "p50= *[0-9.]+ +p95= *[0-9.]+ +p99= *[0-9.]+" "$f" | tail -1 | tr -s ' ' | tr '\n' ' '
    grep -oE "429=[0-9]+|http_429:[0-9]+" "$f" | tail -1 | tr '\n' ' '
    grep -E "^     TOTAL" "$f" | tail -1 | awk '{printf "miss=%s dup=%s ", $4,$5}'
    grep -oE "VERDICT: [A-Z]+" "$f" | tail -1
    echo
  done
done
echo "################ LOAD LADDER DONE $(date -u +%H:%M:%SZ)"
