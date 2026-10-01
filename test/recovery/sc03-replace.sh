#!/usr/bin/env bash
# S3 — one node's disk is gone for good (volume/PVC lost, data unreadable).
# Procedure: remove it from the membership -> start it EMPTY -> add it as a
# learner -> wait until caught up -> promote it. Under load.
#   VICTIM=leader|follower (default follower)   JOIN=1 to start it with QUEEN_RAFT_JOIN=true
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
hr "S3 setup"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 200; $WL consume --group g1 --max 60
$WL load --seconds 90 --rate 40 --tag L3 > runs/s3-load.log 2>&1 &
LOAD=$!; sleep 3
L=$(leader)
if [ "${VICTIM:-follower}" = leader ]; then V=$L; else V=$(( (L + 1) % 3 )); fi
ID=$((V + 1)); OK=$(( (V + 1) % 3 ))
say "leader index $L; victim index $V (node id $ID); operator talks to index $OK"

hr "S3 the disk of node $ID is lost"
$QR wipe "$V"; sleep 4; hs; mem

hr "S3 step 1: remove node $ID from the membership"
api "$OK" DELETE "/api/v1/system/raft/membership/members/$ID"
mem

hr "S3 step 2: start node $ID on an EMPTY volume${JOIN:+ with QUEEN_RAFT_JOIN=true}"
if [ -n "${JOIN:-}" ]; then $QR start "$V" QUEEN_RAFT_JOIN=true; else $QR start "$V"; fi
sleep 5; say "health of the new node: $(h "$V")"
docker logs "queen-mq-v2-$V" 2>&1 | grep -iE 'joins an existing|initializ' | tail -2 | cut -c1-260

hr "S3 step 3: add node $ID as a learner"
api "$OK" POST /api/v1/system/raft/membership/learners "{\"id\":$ID,\"raft\":\"$(FQ "$V"):7400\",\"http\":\"$(FQ "$V"):6632\"}" | cut -c1-300
for i in $(seq 1 60); do sleep 2; m=$(mem); echo "$m" | grep -q "${ID}L(lag=0" && break; case "$m" in *"${ID}L(lag="[0-9]","*|*"${ID}L(lag="[0-9][0-9]","*) break;; esac; done
mem; say "container: $(cstate "$V")"

hr "S3 step 4: promote node $ID"
api "$OK" POST /api/v1/system/raft/membership/promote "{\"ids\":[$ID]}" | cut -c1-300
mem
if [ -n "${JOIN:-}" ]; then
  hr "S3 step 5: restart node $ID without QUEEN_RAFT_JOIN (back to the normal config)"
  $QR start "$V"; wait_healthy "$V" 120
fi
wait_caught_up "$V" 120
wait $LOAD; tail -1 runs/s3-load.log
hr "S3 verify (all nodes, then the rebuilt node alone)"
$WL verify; $WL verify --nodes "$V"; hs; mem
