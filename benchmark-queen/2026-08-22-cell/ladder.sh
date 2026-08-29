#!/bin/bash
# Density ladder for the cell: a REALISTIC SaaS MIX, ramped until the SLO breaks.
#
# SLO (chosen 2026-08-22): the cell is "full" when e2e p99 exceeds 200 ms.
# That is the isolation campaign's measured baseline (p99 120 ms) with headroom,
# and it is the number a tenant actually feels.
#
# The mix, per rung, is three CONCURRENT goload processes against the same cell,
# because -mode cloud drives a homogeneous tenant set and a real cell is not:
#
#   90%  idle    plan free  0.2 msg/s   — the long tail that pays nothing and
#                                          still costs queues, parked pops and
#                                          maintenance. This is the term the
#                                          earlier tenant campaigns showed
#                                          dominating a quiet cell.
#    9%  active  plan pro   5   msg/s   — ordinary paying tenants.
#    1%  noisy   plan pro   100 msg/s   — the loud neighbour, batched 10 so its
#                                          REQUEST rate (10/s) stays inside the
#                                          pro plan's 50 req/s. Without that it
#                                          would 429 and we would be measuring
#                                          the rate limiter, not the cell.
#
# Every tenant keeps one long-polling consumer parked: that is what a real
# consumer does and it is what holds a slot on the proxy.
set -uo pipefail

CELL=10.114.0.2
PROXY="http://$CELL:6711"
OUT=${OUT:-/root/ladder}
DUR=${DUR:-180}
DRAIN=${DRAIN:-20}
# Noisy tenants sit on the `pro` plan (200 msgs/s, 50 req/s). The limiter counts
# messages on the way OUT as well as in, so an offered 100/s measured 429s at
# ~76/s achieved. 50/s with push-batch 10 (5 req/s, ~100 msgs/s counted) leaves
# room. Any 429 at all invalidates a rung: it means we measured the plan, not
# the cell.
NOISY_RATE=${NOISY_RATE:-50}
mkdir -p "$OUT"

# rung: <name> <idle> <active> <noisy>
RUNGS=${RUNGS:-"r100:90:9:1 r300:270:27:3 r1000:900:90:10"}

# ---- warm-up: establish every queue and consumer group ----------------------
# The default subscription mode is `new`: a group created AFTER a push never
# sees that push. In -mode cloud the consumers and producers start together, so
# the first messages of a fresh tenant are skipped and the verifier calls it
# loss (firstMissing=[1 2 3 ...]). A real cell does not look like that — its
# groups already exist — so warm them once and measure afterwards. This keeps
# the production default intact instead of flipping subscriptionMode to `all`.
if [ "${WARMUP:-1}" = "1" ]; then
  echo "################ WARM-UP: establishing queues + consumer groups $(date -u +%H:%M:%SZ)"
  WPIDS=()
  for spec in "idle:/root/idle.json:900" "active:/root/act.json:90" "noisy:/root/noisy.json:10"; do
    IFS=: read -r wl wf wn <<<"$spec"
    /root/goload -mode cloud -target proxy -url "$PROXY" \
      -tenants-file "$wf" -tenants "$wn" -per-tenant-rate 0.5 -push-batch 1 \
      -consumers-per-tenant 1 -pop-wait -pop-timeout 3000 -payload 256 \
      -duration 25 -drain 15 -report 30 -verify=false \
      -out "$OUT" -run-id "warm-$wl" > "$OUT/warm-$wl.log" 2>&1 &
    WPIDS+=($!)
  done
  for p in "${WPIDS[@]}"; do wait "$p" || true; done
  echo "warm-up done $(date -u +%H:%M:%SZ)"
fi

for rung in $RUNGS; do
  IFS=: read -r NAME NIDLE NACT NNOISY <<<"$rung"
  TOTAL=$((NIDLE + NACT + NNOISY))
  echo "################ RUNG $NAME — $TOTAL tenants ($NIDLE idle / $NACT active / $NNOISY noisy) $(date -u +%H:%M:%SZ)"

  # broker-side sampler: per-container, so the TCO split (PG vs brokers vs proxy)
  # falls out of the same run instead of needing a second one.
  ssh -o BatchMode=yes root@$CELL "rm -f /root/samples/$NAME.csv
    setsid nohup /root/sampler.sh cell-pg cell-broker-a cell-broker-b cell-proxy cell-lb \
      > /root/samples/$NAME.csv 2>/dev/null </dev/null &
    echo \$! > /root/cellsampler.pid" >/dev/null

  # Loader-side sampler. SPEC §5.1 voids any run where the load generator was
  # itself the bottleneck, and the isolation campaign proved that is not a
  # theoretical worry: 5000 tenants means 5000 parked long-polls from one box.
  rm -f "$OUT/$NAME-loader.csv"
  setsid nohup /root/cmbench/sampler.sh > "$OUT/$NAME-loader.csv" 2>/dev/null </dev/null &
  echo $! > /root/loadsampler.pid

  # Launched in THIS shell, not a command substitution: a subshell's background
  # job is not a child of the caller and `wait` refuses it.
  PIDS=()
  launch() { # launch <label> <file> <n> <rate> <batch>
    local label=$1 file=$2 n=$3 rate=$4 batch=$5
    [ "$n" -eq 0 ] && return 0
    /root/goload -mode cloud -target proxy -url "$PROXY" \
      -tenants-file "$file" -tenants "$n" \
      -per-tenant-rate "$rate" -push-batch "$batch" \
      -consumers-per-tenant 1 -pop-wait -pop-timeout 5000 \
      -payload 256 -duration "$DUR" -drain "$DRAIN" -report 30 \
      -out "$OUT" -run-id "$NAME-$label" \
      > "$OUT/$NAME-$label.log" 2>&1 &
    PIDS+=($!)
  }

  launch idle   /root/idle.json  "$NIDLE"  0.2 1
  launch active /root/act.json   "$NACT"   5   1
  launch noisy  /root/noisy.json "$NNOISY" "$NOISY_RATE" 10
  for p in "${PIDS[@]}"; do wait "$p" || true; done

  [ -f /root/loadsampler.pid ] && kill $(cat /root/loadsampler.pid) 2>/dev/null
  ssh -o BatchMode=yes root@$CELL '[ -f /root/cellsampler.pid ] && kill $(cat /root/cellsampler.pid) 2>/dev/null; true' >/dev/null
  scp -q root@$CELL:/root/samples/$NAME.csv "$OUT/$NAME-cell.csv" 2>/dev/null

  echo "--- $NAME verdict ---"
  for label in idle active noisy; do
    f="$OUT/$NAME-$label.log"
    [ -f "$f" ] || continue
    printf '  %-7s ' "$label"
    grep -oE "p50= *[0-9.]+ +p95= *[0-9.]+ +p99= *[0-9.]+" "$f" | tail -1 | tr -s ' ' | tr '\n' ' '
    grep -oE "429=[0-9]+|http_429:[0-9]+" "$f" | tail -1 | tr '\n' ' '
    grep -oE "VERDICT: [A-Z]+" "$f" | tail -1
    echo
  done
done
echo "################ LADDER DONE $(date -u +%H:%M:%SZ)"
