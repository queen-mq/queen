#!/usr/bin/env bash
# S11 — a MAJORITY of voters lost their disks for good (2 of 3). No quorum can
# ever form again. QUEEN_RAFT_FORCE_RECOVER=<survivor id> on the survivor makes
# it the only voter; the two others come back EMPTY and rejoin.
# Done the way it goes on Kubernetes: the variable is set for EVERY pod (one
# StatefulSet template) with a stop-all/start-all, then removed the same way.
#   SURVIVOR=follower|leader (which node's disk survives; default follower)
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
hr "S11 setup"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 300 >/dev/null; $WL consume --group g1 --max 60
$WL load --seconds 20 --rate 40 --tag L11 > runs/s11-load.log 2>&1
L=$(leader)
if [ "${SURVIVOR:-follower}" = leader ]; then S=$L; else S=$(( (L + 1) % 3 )); fi
SID=$((S + 1)); others=""; for i in 0 1 2; do [ "$i" != "$S" ] && others="$others $i"; done
say "leader index $L; the survivor is index $S (node $SID); lost:$others"
hr "S11 two disks are lost"
for i in $others; do $QR wipe "$i"; done
sleep 8; hs
st=$(curl -s -m 5 -o /dev/null -w '%{http_code}' -X POST "localhost:1663$S/api/v1/push" -H 'Content-Type: application/json' -d '{"items":[{"queue":"rec.probe","partition":"p","payload":{"x":1}}]}')
say "a push to the survivor answers HTTP $st"
docker logs "queen-mq-v2-$S" 2>&1 | grep -iE 'no leader|quorum|election' | tail -2 | cut -c1-250

hr "S11 step 1: stop everything (k8s: kubectl scale sts --replicas=0)"
for i in 0 1 2; do docker stop -t 30 "queen-mq-v2-$i" >/dev/null 2>&1; done
hr "S11 step 2: start everything with QUEEN_RAFT_FORCE_RECOVER=$SID (k8s: set env, scale to 3)"
for i in 0 1 2; do $QR start "$i" "QUEEN_RAFT_FORCE_RECOVER=$SID" >/dev/null; done
sleep 12; hs
for i in 0 1 2; do say "index $i: $(cstate "$i")"; done
docker logs "queen-mq-v2-$S" 2>&1 | grep -E 'FORCE_RECOVER' | head -2 | cut -c1-400
for i in $others; do say "index $i says: $(docker logs "queen-mq-v2-$i" 2>&1 | grep -oE 'QUEEN_RAFT_FORCE_RECOVER=[0-9]+, but this node is [0-9]+[^"]{0,120}' | tail -1)"; done
mem
say "survivor serves writes now:"; $WL write --tag R --msgs 50 --kv 5 --nodes "$S" | cut -c1-200

hr "S11 step 3: remove QUEEN_RAFT_FORCE_RECOVER everywhere (k8s: unset env, pods restart)"
for i in 0 1 2; do $QR start "$i" >/dev/null; done
wait_healthy "$S" 60; sleep 5; hs; mem
hr "S11 step 4: add the two empty nodes as learners, then promote them"
for i in $others; do
  id=$((i + 1))
  api "$S" POST /api/v1/system/raft/membership/learners "{\"id\":$id,\"raft\":\"$(FQ "$i"):7400\",\"http\":\"$(FQ "$i"):6632\"}" | cut -c1-150
done
for t in $(seq 1 30); do
  r=$(api "$S" POST /api/v1/system/raft/membership/promote "{\"ids\":[$(echo $others | awk '{print $1+1","$2+1}')]}")
  echo "$r" | grep -q '"ok":true' && break; sleep 2
done
echo "$r" | cut -c1-200
for i in $others; do wait_caught_up "$i" 120; done
hs; mem
hr "S11 verify: what survived (acked writes lost are counted, not fatal)"
$WL verify --allow-loss | cut -c1-600
for i in 0 1 2; do $WL verify --nodes "$i" --allow-loss | cut -c1-200; done
