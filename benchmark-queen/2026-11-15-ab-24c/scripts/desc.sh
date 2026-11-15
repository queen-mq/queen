#!/usr/bin/env bash
# desc.sh <tag> <build> <rates...> (controller qab-l1; her ask 11-15 "start high, and decrease if not sustain"):
# a DESCENDING ceiling search on push + pop. One fresh cluster a rate (one ab3.sh call each, so a step that fell
# over leaves nothing behind for the next), from the first rate down, stopping at the first rate carried: the
# ceiling is between that rate and the one above it.
# The workload comes from the environment, as for ab3.sh: TEN, RG, PARTS, BATCH, SPREAD, CONS, PROCS, DUR (40 s).
# Lines to runs/desc.out, prefixed <tag>; each rate's run in runs/<tag>-r<rate>/.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  TAG=$1; B=$2; shift 2
  OUTF=$HERE/runs/desc.out; mkdir -p "$HERE/runs"
  say "desc start: $TAG on $B at $* | TEN=${TEN:-0} RG=${RG:-1} PARTS=${PARTS:-200} BATCH=${BATCH:-100} SPREAD=${SPREAD:-1} PROCS=${PROCS:-3} CONS=${CONS:-15} DUR=${DUR:-40}"
  local R t
  for R in "$@"; do
    t=$TAG-r$R
    bash "$HERE/ab3.sh" "$t" "$B" push "${DUR:-40}" "$R" 2>&1 | tee "$HERE/runs/$t.console" \
      | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED" | sed -u "s/^/$TAG /" >> "$OUTF"
    if grep -q -E '^\[..:..:..\] +[0-9,]+:.* OK *$' "$HERE/runs/$t/run.log" 2>/dev/null; then
      say "$TAG carried $R"; break
    fi
  done
  say "DESC_DONE $TAG"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
main "$@"; exit $?
