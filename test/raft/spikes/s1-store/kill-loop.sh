#!/usr/bin/env bash
# S1 store spike, the PASS/FAIL of WP-0.3: N runs of
#   start -> load -> kill -9 at a random moment during non-durable commits
#   -> reopen -> verify
# and the verdict of each run:
#   * does the applied index the reopened store reports agree with the segment
#     bytes it references (I11)?
#   * did the engine reopen PAST its last durable commit?
#
#   ./kill-loop.sh [runs]
#
# env:
#   ENGINE   redb | fjall | heed          (default redb)
#   RATE     msg/s (default 20000)
#   MIN_MS   earliest kill, ms after start (default 3000: past the first durable point)
#   MAX_MS   latest kill (default 12000)
#   MODE     external | self | mixed      (default mixed)
#            external: kill -9 from here at a random moment (can land INSIDE the
#                      engine's commit, which no fault point can reach)
#            self:     the harness raises SIGKILL at a chosen point of the entry
#                      (append / precommit / postcommit)
#   DATA     where the store dir goes (default /var/tmp/s1-kill)
#   OUT      log directory (default ./results)
#   EXTRA    extra flags for the run
#
# Only PIDs started by this script are killed, by PID, never by port or pattern.
set -u
cd "$(dirname "$0")" || exit 2

RUNS=${1:-10}
ENGINE=${ENGINE:-redb}
RATE=${RATE:-20000}
MIN_MS=${MIN_MS:-3000}
MAX_MS=${MAX_MS:-12000}
MODE=${MODE:-mixed}
DATA=${DATA:-/var/tmp/s1-kill}
OUT=${OUT:-./results}
EXTRA=${EXTRA:-}
BIN=./target/release/s1-store

[ -x "$BIN" ] || { echo "build first: cargo build --release"; exit 2; }
mkdir -p "$OUT" "$DATA"
LOG="$OUT/kill-$ENGINE-$(date +%Y%m%d-%H%M%S).log"
DIR="$DATA/$ENGINE"

pass=0; fail=0; past=0; torn=0; nostate=0
echo "kill-loop: engine=$ENGINE runs=$RUNS rate=$RATE kill in [$MIN_MS,$MAX_MS] ms mode=$MODE" | tee "$LOG"

for i in $(seq 1 "$RUNS"); do
  rm -rf "$DIR"
  span=$(( MAX_MS - MIN_MS )); [ "$span" -lt 1 ] && span=1
  at=$(( MIN_MS + (RANDOM * 32768 + RANDOM) % span ))
  mode=$MODE
  if [ "$mode" = "mixed" ]; then
    if [ $(( i % 2 )) -eq 0 ]; then mode=external; else mode=self; fi
  fi

  if [ "$mode" = "self" ]; then
    point=$(awk -v r=$((RANDOM%3)) 'BEGIN{split("append precommit postcommit",p," ");print p[r+1]}')
    # shellcheck disable=SC2086
    "$BIN" run --engine "$ENGINE" --dir "$DIR" --rate "$RATE" --duration 600 \
      --segment-bytes 262144 --self-kill-after-ms "$at" --kill-point "$point" $EXTRA \
      >"$OUT/.kill-run.log" 2>&1
    rc=$?
    what="self/$point@${at}ms rc=$rc"
  else
    # shellcheck disable=SC2086
    "$BIN" run --engine "$ENGINE" --dir "$DIR" --rate "$RATE" --duration 600 \
      --segment-bytes 262144 $EXTRA >"$OUT/.kill-run.log" 2>&1 &
    pid=$!
    sleep "$(awk -v ms=$at 'BEGIN{printf "%.3f", ms/1000}')"
    kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    rc=$?
    what="external@${at}ms rc=$rc"
  fi

  out=$("$BIN" verify --engine "$ENGINE" --dir "$DIR" --verify-all $EXTRA 2>&1)
  vrc=$?
  line=$(printf '%s\n' "$out" | grep '^VERIFY')
  if [ -z "$line" ]; then
    nostate=$((nostate+1))
    sig=""
    [ "$vrc" -gt 128 ] && sig=" killed by signal $((vrc-128))"
    echo "run $i $what -> VERIFY DID NOT RUN (store would not reopen) rc=$vrc$sig" | tee -a "$LOG"
    printf '%s\n' "$out" | tail -5 | sed 's/^/    /' | tee -a "$LOG"
    continue
  fi
  case "$line" in
    "VERIFY PASS"*) pass=$((pass+1)) ;;
    *) fail=$((fail+1)) ;;
  esac
  case "$line" in *past_durable=true*) past=$((past+1)) ;; esac
  case "$line" in *torn=true*) torn=$((torn+1)) ;; esac
  echo "run $i $what -> $line" | tee -a "$LOG"
done

echo | tee -a "$LOG"
echo "SUMMARY engine=$ENGINE runs=$RUNS pass=$pass fail=$fail reopened_past_durable=$past torn_entries=$torn no_reopen=$nostate" | tee -a "$LOG"
rm -rf "$DIR"
[ "$fail" -eq 0 ] && [ "$nostate" -eq 0 ]
