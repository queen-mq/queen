#!/bin/bash
# Ceiling hunt, commercial mix: 500 free / 300 dev / 200 pro per 1000 tenants.
#
# Differences from the first ladder, all deliberate:
#   * 50% of tenants pay, not 10%. This is the mix the pricing question is
#     actually about.
#   * TEN partitions per queue instead of one, so an ordering key is not the
#     whole queue and the per-queue maintenance term is 10x larger.
#   * -pop-partitions 10 to match. The isolation campaign showed that a visit
#     width below the partition count starves the tail partitions and shows up
#     as GAPS, not as latency — a config error that reads like data loss.
#
# Per-tier rates are chosen to sit well inside each plan's limits, because a
# 429 means we measured the limiter instead of the cell:
#   free 0.2 msg/s (cap 20)   dev 2 msg/s (cap 40)   pro 10 msg/s (cap 200)
# Remember the limiter counts messages OUTBOUND too, so the effective push
# ceiling is about half the plan's nominal rate.
set -uo pipefail

CELL=10.114.0.2
PROXY="http://$CELL:6711"
OUT=${OUT:-/root/mix2}
DUR=${DUR:-180}
DRAIN=${DRAIN:-60}
PARTS=${PARTS:-10}
mkdir -p "$OUT"

# rung: <name> <free> <dev> <pro>
RUNGS=${RUNGS:-"m5000:2500:1500:1000 m10000:5000:3000:2000"}

if [ "${WARMUP:-1}" = "1" ]; then
  echo "################ WARM-UP (groups + partitions) $(date -u +%H:%M:%SZ)"
  WP=()
  for spec in "free:/root/t-free.json:5000" "dev:/root/t-dev.json:3000" "pro:/root/t-pro.json:2000"; do
    IFS=: read -r wl wf wn <<<"$spec"
    /root/goload -mode cloud -target proxy -url "$PROXY" \
      -tenants-file "$wf" -tenants "$wn" -per-tenant-rate 0.5 -push-batch 1 \
      -partitions "$PARTS" -pop-partitions "$PARTS" \
      -consumers-per-tenant 1 -pop-wait -pop-timeout 3000 -payload 256 \
      -duration 30 -drain 20 -report 30 -verify=false \
      -out "$OUT" -run-id "warm-$wl" > "$OUT/warm-$wl.log" 2>&1 &
    WP+=($!)
  done
  for p in "${WP[@]}"; do wait "$p" || true; done
  echo "warm-up done $(date -u +%H:%M:%SZ)"
fi

for rung in $RUNGS; do
  IFS=: read -r NAME NFREE NDEV NPRO <<<"$rung"
  TOTAL=$((NFREE + NDEV + NPRO))
  echo "################ RUNG $NAME — $TOTAL tenants ($NFREE free / $NDEV dev / $NPRO pro), ${PARTS} partitions/queue  $(date -u +%H:%M:%SZ)"

  ssh -o BatchMode=yes root@$CELL "rm -f /root/samples/$NAME.csv
    setsid nohup /root/sampler.sh cell-pg cell-broker-a cell-broker-b cell-proxy cell-lb \
      > /root/samples/$NAME.csv 2>/dev/null </dev/null &
    echo \$! > /root/cellsampler.pid" >/dev/null

  rm -f "$OUT/$NAME-loader.csv"
  setsid nohup /root/cmbench/sampler.sh > "$OUT/$NAME-loader.csv" 2>/dev/null </dev/null &
  echo $! > /root/loadsampler.pid

  PIDS=()
  launch() { # <label> <file> <n> <rate> <batch>
    local label=$1 file=$2 n=$3 rate=$4 batch=$5
    [ "$n" -eq 0 ] && return 0
    /root/goload -mode cloud -target proxy -url "$PROXY" \
      -tenants-file "$file" -tenants "$n" \
      -per-tenant-rate "$rate" -push-batch "$batch" \
      -partitions "$PARTS" -pop-partitions "$PARTS" \
      -consumers-per-tenant 1 -pop-wait -pop-timeout 5000 \
      -payload 256 -duration "$DUR" -drain "$DRAIN" -report 30 \
      -out "$OUT" -run-id "$NAME-$label" \
      > "$OUT/$NAME-$label.log" 2>&1 &
    PIDS+=($!)
  }
  launch free /root/t-free.json "$NFREE" 0.2 1
  launch dev  /root/t-dev.json  "$NDEV"  2   1
  launch pro  /root/t-pro.json  "$NPRO"  10  2
  for p in "${PIDS[@]}"; do wait "$p" || true; done

  [ -f /root/loadsampler.pid ] && kill $(cat /root/loadsampler.pid) 2>/dev/null
  ssh -o BatchMode=yes root@$CELL '[ -f /root/cellsampler.pid ] && kill $(cat /root/cellsampler.pid) 2>/dev/null; true' >/dev/null
  scp -q root@$CELL:/root/samples/$NAME.csv "$OUT/$NAME-cell.csv" 2>/dev/null

  echo "--- $NAME verdict ---"
  for label in free dev pro; do
    f="$OUT/$NAME-$label.log"
    [ -f "$f" ] || continue
    printf '  %-5s ' "$label"
    grep -oE "p50= *[0-9.]+ +p95= *[0-9.]+ +p99= *[0-9.]+" "$f" | tail -1 | tr -s ' ' | tr '\n' ' '
    grep -oE "429=[0-9]+|http_429:[0-9]+" "$f" | tail -1 | tr '\n' ' '
    grep -E "^     TOTAL" "$f" | tail -1 | awk '{printf "sent=%s recv=%s miss=%s dup=%s ", $2,$3,$4,$5}'
    grep -oE "VERDICT: [A-Z]+" "$f" | tail -1
    echo
  done
done
echo "################ MIX2 LADDER DONE $(date -u +%H:%M:%SZ)"
