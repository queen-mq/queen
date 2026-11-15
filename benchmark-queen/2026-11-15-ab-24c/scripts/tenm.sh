#!/usr/bin/env bash
# tenm.sh (controller qab-l1): the 10M-partition row of the 2026-10-13 partitions matrix
# (../../2026-10-12-three-node-new-forms/README.md, grid3.sh) on both builds, AFTER abplan.sh: it waits for
# ABPLAN_DONE in runs/abplan.out (WAIT=0 skips the wait). Every point is a fresh cluster (one ab3.sh call per rate):
# one queue of 10M partitions, configured with deduplication off and completed messages kept 1,800 s, warmed with one
# message a partition and drained (the drain may wait up to WARM_IDLE, 300 s, with nothing out: on 10-13 the first
# pops on ten million partitions took over a minute; a warm-up that does not drain ends the build's row); then 90 s of load: 9 processes x 33 consumers, pop width 10, batch 1000, 100
# messages a push, the binary forms, and QUEEN_CONSUME_IDLE_UNLOAD_S=30 as on 10-13 (the default is 600). Rates
# ascending, the two builds alternating at each rate; a build stops at its first rate not carried.
# Lines to runs/tenm.out, prefixed tenm-<build> (abtable.py reads them as one workload).
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  OUTF=$HERE/runs/tenm.out; mkdir -p "$HERE/runs"
  RATES=${RATES:-"500000 1000000 1500000 2000000 2500000"}
  if [ "${WAIT:-1}" = 1 ]; then
    say "tenm waiting for abplan.sh to end"
    until grep -q ABPLAN_DONE "$HERE/runs/abplan.out" 2>/dev/null; do sleep 30; done
  fi
  say "tenm start: 10M partitions at $RATES | builds 281 new"
  local stop281=0 stopnew=0 b tag var
  for R in $RATES; do
    for b in 281 new; do
      var=stop$b; [ "${!var}" = 1 ] && continue
      tag=tenm-$b-r$R
      say "== 10M partitions at $R on $b"
      PARTS=10000000 CONS=33 SPREAD=1 CONFIGURE=1 CRET=1800 WARM_IDLE=${WARM_IDLE:-300s} EXTRA_ENV="QUEEN_CONSUME_IDLE_UNLOAD_S=30" \
        bash "$HERE/ab3.sh" $tag $b push 90 $R 2>&1 | tee "$HERE/runs/$tag.console" \
        | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|warm|create" | sed -u "s/^/tenm-$b /" >> "$OUTF"
      grep -q -E '^\[..:..:..\] +[0-9,]+:.* OK *$' "$HERE/runs/$tag/run.log" 2>/dev/null || { printf -v $var 1; say "tenm-$b stops at $R"; }
    done
  done
  say "TENM_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
main "$@"; exit $?
