#!/usr/bin/env bash
# smoke.sh (controller qab-l1): three 20 s steps before the plan: plain push on new, one filtered group (data.k < 30)
# on new, one plain consumer group on 2.8.1. Lines to runs/smoke.out.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); OUTF=$HERE/runs/smoke.out; mkdir -p "$HERE/runs"; : > "$OUTF"
run() { local tag=$1; shift; env "$@" 2>&1 | tee "$HERE/runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping|run $tag" | sed -u "s/^/$tag /" >> "$OUTF"; }
run smoke-push PARTS=1000 CONS=15 bash "$HERE/ab3.sh" smoke-push new push 20 1000000
run smoke-f1 PARTS=1000 CONS=15 GROUPS_ARGS="-group-where 'data.k < 30' -stamp-k 100" SHARE=0.3 bash "$HERE/ab3.sh" smoke-f1 new push 20 500000
run smoke-g1 PARTS=1000 CONS=15 GROUPS_ARGS="-groups 1" SHARE=1 bash "$HERE/ab3.sh" smoke-g1 281 push 20 500000
echo SMOKE_DONE >> "$OUTF"
