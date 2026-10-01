#!/usr/bin/env bash
# K2b (Kubernetes) — what really happens when two PVCs vanish while the
# StatefulSet keeps running (disks gone, PVCs deleted): the StatefulSet recreates
# the two pods on EMPTY volumes under the old ids. Observe for 60 s, verify.
set -uo pipefail
. "$(dirname "$0")/k8s-lib.sh"
cd "$REC"; rm -rf state
$WL write --tag A --msgs 300 >/dev/null; $WL consume --group g1 --max 60
L=$(kleader); S=$(( (L + 1) % 3 ))
lost=""; for i in 0 1 2; do [ "$i" != "$S" ] && lost="$lost $i"; done
say "leader pod $L; survivor $S; lost:$lost"
hr "K2b two PVCs + pods deleted (the leader's included)"
for i in $lost; do k delete pvc "data-queen-mq-v2-$i" --wait=false >/dev/null; done
for i in $lost; do k delete pod "queen-mq-v2-$i" --wait=false >/dev/null; done
for t in $(seq 1 12); do sleep 5; echo "--- +$((t*5))s"; pods; for i in 0 1 2; do printf '  %s: %s\n' "$i" "$(k exec queen-mq-v2-$i -c queen-mq-v2 -- curl -s -m 2 localhost:6632/health 2>/dev/null | cut -c1-170)"; done; done
kmem "queen-mq-v2-$S"
$WL verify --allow-loss 2>&1 | cut -c1-300
