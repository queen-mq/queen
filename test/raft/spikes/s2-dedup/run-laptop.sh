#!/usr/bin/env bash
# S2 (WP-0.4) laptop campaign: option (a) vs option (b), 5 minutes each.
#
#   ./run-laptop.sh            # the whole matrix, ~35 min
#   DUR=60 ./run-laptop.sh     # a quick pass
#
# Two window scales, because they answer different questions:
#   short  window 60 s, txns 72 s, retention 20 s, duplicate ages 0..70 s —
#          the run (5 min) is much longer than the window, so pruning, file GC
#          and hash-only compaction all reach steady state AND duplicates land
#          on both sides of the window (the "none outside falsely" half of
#          exactness needs ages > window).
#   long   window 300 s (the 5 min the task asks for), txns 360 s, retention
#          60 s, ages 0..350 s. Nothing expires inside a 5 min run, so this one
#          shows the growth phase only.
#
# Every run is followed by a restart (`rebuild`), which is the measurement of
# "how long before this node can answer a dedup probe again" plus an exactness
# check of the samples the run saved.
set -u
cd "$(dirname "$0")" || exit 2

DUR=${DUR:-300}
DATA=${DATA:-/var/tmp/s2-dedup}
OUT=${OUT:-./results}
BIN=./target/release/s2-dedup
[ -x "$BIN" ] || { echo "build first: cargo build --release"; exit 2; }
mkdir -p "$OUT" "$DATA"

COMMON="--engine fjall --rate 50000 --batch 10 --entry-appends 10 --payload 96 \
 --partitions 4096 --segment-bytes 2097152 --fsync-mode data --fsync-threads 8 --duration $DUR"
SHORT="--window-s 60 --txns-s 72 --retention-s 20 --dup-max-age-s 70"
LONG="--window-s 300 --txns-s 360 --retention-s 60 --dup-max-age-s 350"

run() { # name, args...
  local name=$1; shift
  local d="$DATA/$name"
  rm -rf "$d"
  echo "=== run $name  $(date +%H:%M:%S)"
  # shellcheck disable=SC2086
  "$BIN" run --dir "$d" $COMMON "$@" --label "$name" >"$OUT/$name.log" 2>&1
  echo "    rc=$? $(grep -c . "$OUT/$name.log") lines"
  grep -E '^RESULT' "$OUT/$name.log" || true
}

rebuild() { # name, mode, args...
  local name=$1 mode=$2; shift 2
  local d="$DATA/$name"
  echo "=== rebuild $name mode=$mode  $(date +%H:%M:%S)"
  # shellcheck disable=SC2086
  "$BIN" rebuild --dir "$d" --engine fjall --payload 96 --segment-bytes 2097152 \
     --mode "$mode" "$@" >"$OUT/$name-rebuild-$mode.log" 2>&1
  echo "    rc=$?"
  grep -E '^RESULT' "$OUT/$name-rebuild-$mode.log" || true
}

drop() { rm -rf "$DATA/$1"; }

# ---- short window: steady state, both directions of exactness ----
run a-short --option a $SHORT
rebuild a-short blooms --option a --window-s 60
drop a-short

run b-short --option b $SHORT --cache-mb 64
rebuild b-short blooms --option b --window-s 60 --cache-mb 64
rebuild b-short files --option b --window-s 60 --cache-mb 64
drop b-short

run b-short-nocache --option b $SHORT --cache-mb 0
drop b-short-nocache

run b-short-earlystop --option b $SHORT --cache-mb 64 --ack-early-stop 1
drop b-short-earlystop

run b-short-noack --option b $SHORT --cache-mb 64 --ack-rate 0
drop b-short-noack

# ---- the 5-minute window the task names ----
run a-long --option a $LONG
rebuild a-long blooms --option a --window-s 300
drop a-long

run b-long --option b $LONG --cache-mb 64
rebuild b-long blooms --option b --window-s 300 --cache-mb 64
drop b-long

echo "=== done $(date +%H:%M:%S)"
