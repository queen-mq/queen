#!/usr/bin/env bash
# K1 (Kubernetes) — replace a node whose disk is lost: the runbook's commands,
# against the real helm_v2 chart on k3s.
set -uo pipefail
. "$(dirname "$0")/k8s-lib.sh"
cd "$REC"; rm -rf state
hr "K1 setup"
pods
$WL write --tag A --msgs 300 >/dev/null; $WL consume --group g1 --max 60
$WL load --seconds 120 --rate 20 --tag K1 > runs/k1-load.log 2>&1 &
LOAD=$!
L=$(kleader); V=$(( (L + 1) % 3 )); OK=$(( (V + 1) % 3 )); ID=$((V + 1))
say "leader pod queen-mq-v2-$L; victim queen-mq-v2-$V (node $ID); commands through queen-mq-v2-$OK"
hr "K1 step 1: remove node $ID from the membership"
q "queen-mq-v2-$OK" "/api/v1/system/raft/membership/members/$ID" -X DELETE | cut -c1-200
kmem "queen-mq-v2-$OK"
hr "K1 step 2: delete the PVC, then the pod (the StatefulSet recreates both, empty)"
t0=$(date +%s)
k delete pvc "data-queen-mq-v2-$V" --wait=false
k delete pod "queen-mq-v2-$V" --wait=true
for i in $(seq 1 60); do sleep 2; s=$(k get pod "queen-mq-v2-$V" --no-headers 2>/dev/null | awk '{print $3}'); [ "$s" = Running ] && break; done
say "new pod Running after $(( $(date +%s) - t0 ))s"; pods; k get pvc
k logs "queen-mq-v2-$V" -c queen-mq-v2 2>/dev/null | grep -E 'joins an existing' | cut -c1-200
hr "K1 step 3: add node $ID as a learner"
q "queen-mq-v2-$OK" /api/v1/system/raft/membership/learners -X POST -H 'Content-Type: application/json' \
  -d "{\"id\":$ID,\"raft\":\"$(FQK "$V"):7400\",\"http\":\"$(FQK "$V"):6632\"}" | cut -c1-200
hr "K1 step 4: promote it (retry while it catches up)"
for i in $(seq 1 30); do
  r=$(q "queen-mq-v2-$OK" /api/v1/system/raft/membership/promote -X POST -H 'Content-Type: application/json' -d "{\"ids\":[$ID]}")
  echo "$r" | grep -q '"ok":true' && break; echo "$r" | grep -o '"code":"[a-z_]*"'; sleep 3
done
kmem "queen-mq-v2-$OK"; wait_ready "queen-mq-v2-$V" 120; pods
say "total $(( $(date +%s) - t0 ))s"
wait $LOAD; tail -1 runs/k1-load.log
hr "K1 verify (all pods, then the rebuilt pod alone)"
$WL verify | cut -c1-300; $WL verify --nodes "$V" | cut -c1-300
