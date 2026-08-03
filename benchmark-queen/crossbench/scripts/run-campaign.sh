#!/usr/bin/env bash
# CM-BENCH campaign driver: the three sweeps of SPEC.md §6, for one system.
#
#   ./run-campaign.sh pgmq  --pgmq-dsn 'postgres://postgres:postgres@BROKER:5432/postgres'
#   ./run-campaign.sh kafka --kafka-seeds BROKER:9092
#   ./run-campaign.sh rabbit --rabbit-url amqp://guest:guest@BROKER:5672/ --lanes 100
#
# Everything after the system name is passed through to cmbench, so per-system
# flags and any tuning you want on the record go there.
#
# Run this from the LOADER VM. Start scripts/sampler.sh on the broker VM (and on
# this one) first: without samples there is no cost table, only a throughput
# number, and a throughput number is the thing this campaign exists to not be.
set -euo pipefail

SYSTEM="${1:-}"
if [ -z "$SYSTEM" ]; then
  echo "usage: $0 <mem|kafka|rabbit|pgmq> [cmbench flags...]" >&2
  exit 2
fi
shift

BIN="${CM_BIN:-./cmbench}"
OUT="${CM_OUT:-./results/$SYSTEM}"
RATE="${CM_RATE:-5000}"
PROPS="${CM_PROPS:-1000}"
DURATION="${CM_DURATION:-1200}"
RAMP="${CM_RAMP:-30}"
DRAIN="${CM_DRAIN:-90}"

mkdir -p "$OUT"

run() { # run <label> <extra flags...>
  local label="$1"; shift
  local dir="$OUT/$label"
  mkdir -p "$dir"
  echo
  echo "################ $SYSTEM :: $label ################"
  echo "# $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # Record the exact invocation next to its logs: SPEC.md §5.2 requires every
  # non-default setting to be justified, and §5.6 requires it to be published.
  echo "$BIN -system $SYSTEM $* $EXTRA" > "$dir/invocation.txt"

  set +e
  "$BIN" -system "$SYSTEM" -logdir "$dir" "$@" $EXTRA 2>&1 | tee "$dir/run.log"
  local rc=${PIPESTATUS[0]}
  set -e
  echo "exit=$rc" >> "$dir/invocation.txt"
  case $rc in
    0) echo "=> PASS" ;;
    3) echo "=> FAIL (correctness) — this is a RESULT, keep it and publish it" ;;
    *) echo "=> ERROR rc=$rc" ;;
  esac
  return 0
}

EXTRA="$*"

# ---------------------------------------------------------------------------
# 0. Control. A clean mem run on this loader licenses attributing later defects
#    to the system rather than to the rig. Cheap; never skip it.
# ---------------------------------------------------------------------------
if [ "${CM_SKIP_CONTROL:-0}" != "1" ]; then
  echo "################ CONTROL (in-memory reference broker) ################"
  mkdir -p "$OUT/../control"
  "$BIN" -system mem -rate 500 -properties 50 -duration 20 -ramp 0 -drain 30 \
         -logdir "$OUT/../control" 2>&1 | tail -20
  echo "control run finished; a FAIL above invalidates everything below"
fi

# ---------------------------------------------------------------------------
# 1. Cost to serve (SPEC.md §6.1) — fixed rate, fixed cardinality, full duration.
#    This is the primary table.
#
#    Systems with a consumption-strategy axis run it BOTH ways and publish the
#    pair (SPEC.md §5.5). For Queen that is wildcard (dynamic lanes, pays the
#    candidate scan) and targeted (static ownership, pushes the map into the
#    app). Quoting one without the other is not a result.
# ---------------------------------------------------------------------------
if [ "$SYSTEM" = "queen" ]; then
  for mode in wildcard targeted; do
    run "fixed-r${RATE}-${mode}" -rate "$RATE" -properties "$PROPS" \
        -duration "$DURATION" -ramp "$RAMP" -drain "$DRAIN" \
        -queen-pop-mode "$mode"
  done
else
  run "fixed-r${RATE}" -rate "$RATE" -properties "$PROPS" \
      -duration "$DURATION" -ramp "$RAMP" -drain "$DRAIN"
fi

# ---------------------------------------------------------------------------
# 2. Ceiling (SPEC.md §6.2) — climb until correctness breaks, on the same box.
#    Shorter runs: this is a search, not a soak. The last PASS is the ceiling.
# ---------------------------------------------------------------------------
if [ "${CM_SKIP_CEILING:-0}" != "1" ]; then
  for r in ${CM_CEILING_RATES:-5000 8000 12000 18000 25000}; do
    run "ceiling-r${r}" -rate "$r" -properties "$PROPS" \
        -duration "${CM_CEILING_DURATION:-300}" -ramp 20 -drain 60
  done
fi

# ---------------------------------------------------------------------------
# 3. Cardinality sweep (SPEC.md §6.3) — fixed rate, varying lane count.
#    This is what separates cost-per-message from cost-per-lane and yields the
#    two-term cost model. It is the cheapest, highest-value sweep here.
# ---------------------------------------------------------------------------
if [ "${CM_SKIP_CARDINALITY:-0}" != "1" ]; then
  for p in ${CM_CARDINALITIES:-100 1000 5000}; do
    run "cardinality-p${p}" -rate "$RATE" -properties "$p" \
        -duration "${CM_CARD_DURATION:-600}" -ramp "$RAMP" -drain "$DRAIN"
  done
fi

echo
echo "################ done: $SYSTEM ################"
echo "results under $OUT"
echo "each subdirectory holds: run.log, invocation.txt, result.json, 12 stage logs, produced.meta"
echo
echo "re-verify any run offline with:"
echo "  $BIN -verify-only $OUT/<label> -properties <P>"
