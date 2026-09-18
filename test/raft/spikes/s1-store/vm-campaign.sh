#!/usr/bin/env bash
# S1 store spike, WP-0.3 part 2: the whole VM campaign in one driver.
# LINUX, ROOT (step 3 needs losetup + device-mapper).
#
#   ./vm-campaign.sh [short|full] [steps]
#
# steps is a comma list of matrix,kill,flaky,soak (default: all four, in order).
#
#   short   what fits in a 45-minute budget: 90 s per matrix cell, 15 kill runs
#           and 5 flaky runs per engine, a 10-minute soak.   (ran 2026-09-17)
#   full    what WP-0.3 asks for and `short` cannot show: 2 h per matrix cell,
#           100 kill runs, 20 flaky runs, a 2 h soak.        (DEFERRED)
#
# env overrides: ENGINES, RATES, TOP2 ("fjall heed"), LEAD, SOAK_RATE, OUT, DATA.
#
# Everything lands in $OUT (default ./results-vm). Nothing is left running and
# no loop or dm device survives: flaky.sh cleans up its own in an EXIT trap.
set -u
cd "$(dirname "$0")" || exit 2

PROFILE=${1:-short}
STEPS=${2:-matrix,kill,flaky,soak}
BIN=./target/release/s1-store
[ -x "$BIN" ] || { echo "build first: cargo build --release"; exit 2; }
[ "$(uname -s)" = "Linux" ] || echo "WARNING: not Linux; the flaky step will refuse"

case "$PROFILE" in
  short) MATRIX_DUR=90;   KILL_RUNS=15;  FLAKY_RUNS=5;  FLAKY_RUN_S=20; SOAK_S=600 ;;
  full)  MATRIX_DUR=7200; KILL_RUNS=100; FLAKY_RUNS=20; FLAKY_RUN_S=60; SOAK_S=7200 ;;
  *) echo "profile must be short|full"; exit 2 ;;
esac

export ENGINES=${ENGINES:-"redb fjall heed"}
export RATES=${RATES:-"20000 50000 100000"}
export OUT=${OUT:-./results-vm}
export DATA=${DATA:-/root/raft/s1-store/data}
SOAK_RATE=${SOAK_RATE:-50000}
# Small roll size so files actually roll and reclamation is observable inside a
# short run (the plan's default is 64 MiB, which never rolls in 90 s).
MATRIX_SEG=${MATRIX_SEG:-262144}
SOAK_SEG=${SOAK_SEG:-4194304}

mkdir -p "$OUT" "$DATA"
STAMP=$(date +%Y%m%d-%H%M%S)
JOURNAL="$OUT/campaign-$STAMP.log"
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$JOURNAL"; }
has() { case ",$STEPS," in *",$1,"*) return 0 ;; *) return 1 ;; esac }

say "campaign profile=$PROFILE steps=$STEPS host=$(hostname) kernel=$(uname -r)"
say "engines=$ENGINES rates=$RATES matrix_dur=${MATRIX_DUR}s kill_runs=$KILL_RUNS flaky_runs=$FLAKY_RUNS soak=${SOAK_S}s"
T0=$(date +%s)

# ------------------------------------------------------------------ matrix ---
if has matrix; then
  say "STEP 1 matrix: $(echo "$ENGINES" | wc -w) engines x $(echo "$RATES" | wc -w) rates x ${MATRIX_DUR}s"
  EXTRA="--segment-bytes $MATRIX_SEG" ./run-matrix.sh "$MATRIX_DUR" 2>&1 | tee -a "$JOURNAL"
  say "STEP 1 done after $(( $(date +%s) - T0 ))s"
fi

# The two engines to carry into the crash steps: highest total achieved msg/s.
if [ -n "${TOP2:-}" ]; then
  top2="$TOP2"
else
  tsv=$(ls -t "$OUT"/matrix-*.tsv 2>/dev/null | head -1)
  if [ -n "$tsv" ]; then
    top2=$(awk '
      function g(k,  i,n) { for (i=1;i<=NF;i++) { n=index($i,"="); if (substr($i,1,n-1)==k) return substr($i,n+1) } return 0 }
      /^RESULT/ { s[g("engine")] += g("msgs_per_s") }
      END { for (e in s) printf "%.0f %s\n", s[e], e }' "$tsv" | sort -rn | head -2 | awk '{print $2}' | tr '\n' ' ')
  else
    top2="fjall heed"
  fi
fi
LEAD=${LEAD:-$(echo "$top2" | awk '{print $1}')}
say "top two engines by achieved msg/s: $top2 ; leading engine for the soak: $LEAD"

# -------------------------------------------------------------------- kill ---
if has kill; then
  say "STEP 2 kill -9 loop: $KILL_RUNS runs for each of [$top2]"
  for e in $top2; do
    ENGINE=$e RATE=20000 MODE=mixed DATA=$DATA/kill ./kill-loop.sh "$KILL_RUNS" 2>&1 | tee -a "$JOURNAL"
    say "  kill-loop $e rc=$?"
  done
  say "STEP 2 done after $(( $(date +%s) - T0 ))s"
fi

# ------------------------------------------------------------------- flaky ---
if has flaky; then
  say "STEP 3 dropped unflushed writes (dm-flakey): $FLAKY_RUNS runs for each of [$top2]"
  for e in $top2; do
    ENGINE=$e RATE=20000 RUN_S=$FLAKY_RUN_S BASE=/root/raft/s1-store/flaky ./flaky.sh "$FLAKY_RUNS" 2>&1 | tee -a "$JOURNAL"
    say "  flaky $e rc=$?"
  done
  losetup -a | tee -a "$JOURNAL"
  dmsetup ls 2>&1 | tee -a "$JOURNAL"
  say "STEP 3 done after $(( $(date +%s) - T0 ))s"
fi

# -------------------------------------------------------------------- soak ---
if has soak; then
  del_at=$(( SOAK_S * 6 / 10 ))
  say "STEP 4 soak: $LEAD at $SOAK_RATE msg/s for ${SOAK_S}s, RSS every 5 s, growth every 30 s, delete 30% at ${del_at}s"
  log="$OUT/soak-$LEAD-$SOAK_RATE-$STAMP.log"
  rm -rf "$DATA/soak-$LEAD"
  "$BIN" run --engine "$LEAD" --dir "$DATA/soak-$LEAD" --rate "$SOAK_RATE" --duration "$SOAK_S" \
    --segment-bytes "$SOAK_SEG" --sample-ms 5000 --growth-every-s 30 \
    --delete-at-s "$del_at" --delete-pct 30 >"$log" 2>&1
  say "  soak rc=$? -> $log"
  grep -E '^(GROWTH|MIDDELETE|RESULT)' "$log" | tee -a "$JOURNAL"
  rm -rf "$DATA/soak-$LEAD"
  say "STEP 4 done after $(( $(date +%s) - T0 ))s"
fi

say "campaign finished in $(( $(date +%s) - T0 ))s; journal $JOURNAL"
