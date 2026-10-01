#!/usr/bin/env bash
# K2c (Kubernetes) — majority lost, recovered pod by pod with updateStrategy
# OnDelete (no rolling update can stall, nothing restarts unless deleted, no
# placement race): the survivor gets QUEEN_RAFT_FORCE_RECOVER, the others are
# rebuilt empty one at a time. Runs on whatever state the cluster is in.
#   SURVIVOR=<ordinal of the pod whose data survived>
set -uo pipefail
. "$(dirname "$0")/k8s-lib.sh"
cd "$REC"
S="${SURVIVOR:?ordinal of the surviving pod}"; SID=$((S + 1))
others=""; for i in 0 1 2; do [ "$i" != "$S" ] && others="$others $i"; done
t0=$(date +%s)
hr "1. updateStrategy OnDelete (template changes reach a pod only when it is deleted)"
k patch sts queen-mq-v2 -p '{"spec":{"updateStrategy":{"type":"OnDelete","rollingUpdate":null}}}'
hr "2. QUEEN_RAFT_FORCE_RECOVER=$SID in the template, restart ONLY the survivor queen-mq-v2-$S"
k set env sts/queen-mq-v2 QUEEN_RAFT_FORCE_RECOVER=$SID
k delete pod "queen-mq-v2-$S"
wait_ready "queen-mq-v2-$S" 180
k logs "queen-mq-v2-$S" -c queen-mq-v2 | grep -o 'QUEEN_RAFT_FORCE_RECOVER: UNSAFE RECOVERY[^"]\{0,80\}' | head -1
kmem "queen-mq-v2-$S"
hr "3. remove the variable from the template, restart the survivor again"
k set env sts/queen-mq-v2 QUEEN_RAFT_FORCE_RECOVER-
k delete pod "queen-mq-v2-$S"
wait_ready "queen-mq-v2-$S" 180; kmem "queen-mq-v2-$S"
hr "4. rebuild the other pods empty, one at a time: PVC + pod, learner, promote"
for i in $others; do
  id=$((i + 1))
  k delete pvc "data-queen-mq-v2-$i" --wait=false
  k delete pod "queen-mq-v2-$i"
  for t in $(seq 1 60); do sleep 2; [ "$(k get pod "queen-mq-v2-$i" -o jsonpath='{.status.phase}' 2>/dev/null)" = Running ] && break; done
  q "queen-mq-v2-$S" /api/v1/system/raft/membership/learners -X POST -H 'Content-Type: application/json' \
    -d "{\"id\":$id,\"raft\":\"$(FQK "$i"):7400\",\"http\":\"$(FQK "$i"):6632\"}" | grep -o '"ok":true\|"code":"[a-z_]*"'
  for t in $(seq 1 30); do
    r=$(q "queen-mq-v2-$S" /api/v1/system/raft/membership/promote -X POST -H 'Content-Type: application/json' -d "{\"ids\":[$id]}")
    echo "$r" | grep -q '"ok":true' && { say "node $id promoted"; break; }; sleep 3
  done
  wait_ready "queen-mq-v2-$i" 120
done
hr "5. back to RollingUpdate (helm upgrade does the same)"
k patch sts queen-mq-v2 -p '{"spec":{"updateStrategy":{"type":"RollingUpdate"}}}'
pods; kmem "queen-mq-v2-$S"
say "procedure took $(( $(date +%s) - t0 ))s"
hr "verify"
$WL verify --allow-loss | cut -c1-300; for i in 0 1 2; do $WL verify --nodes "$i" --allow-loss | cut -c1-110; done
