#!/usr/bin/env bash
# after-tenm.sh (controller qab-l1): what runs after tenm.sh. Waits for TENM_DONE in runs/tenm.out, then ONLY="$1"
# (default: single = one message a push request) through abplan.sh on both builds; lines go to runs/abplan.out.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
until grep -q TENM_DONE "$HERE/runs/tenm.out" 2>/dev/null; do sleep 30; done
ONLY="${1:-single}" exec "$HERE/abplan.sh"
