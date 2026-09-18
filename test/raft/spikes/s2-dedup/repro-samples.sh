#!/usr/bin/env bash
# The laptop half of RESULTS-vm.md sec 3: reproduce the rebuild `wrong=` counts
# the first VM campaign reported, then show them gone.
#
# Scaled windows so a 2-minute run has the shape of a 10-minute one at the
# campaign's windows: the duplicate ages (90 s) reach far past the dedup window
# (40 s), and the run outlives window + txns, so a hash re-pushed mid-run still
# has a YOUNG occurrence at the end while the sample's own occurrence is long
# expired. Logs: results/repro-samples-*.log
set -u
cd "$(dirname "$0")" || exit 2
BIN=./target/release/s2-dedup
D=${D:-/var/tmp/s2-dedup-repro}
W="--window-s 40 --txns-s 50 --retention-s 10 --dup-max-age-s 90"
for mode in legacy fixed; do
  flag=false; [ "$mode" = legacy ] && flag=true
  rm -rf "$D-$mode"
  $BIN run --dir "$D-$mode" --engine fjall --option a --rate 50000 --batch 10 --entry-appends 10 \
     --payload 96 --partitions 4096 --segment-bytes 2097152 --fsync-mode data --fsync-threads 8 \
     --duration 120 $W --legacy-samples $flag --label "repro-$mode" \
     > "results/repro-samples-$mode.log" 2>&1
  echo "run $mode rc=$?"
  $BIN rebuild --dir "$D-$mode" --engine fjall --option a --payload 96 --segment-bytes 2097152 \
     --mode blooms --window-s 40 > "results/repro-samples-$mode-rebuild.log" 2>&1
  echo "rebuild $mode rc=$? (legacy is expected to be 1, fixed 0)"
  grep -E "samples checked" "results/repro-samples-$mode-rebuild.log"
  rm -rf "$D-$mode"
done
