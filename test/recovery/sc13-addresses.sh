#!/usr/bin/env bash
# S13 — every node's raft address changed at once (release/namespace/headless
# service renamed, cluster domain changed, hosts moved) while the DATA was kept.
# The membership in the log still names the old addresses, so nobody can reach
# anybody: no leader, ever. Recovery: QUEEN_RAFT_FORCE_RECOVER on ONE node (it
# takes its new address from QUEEN_RAFT_PEERS), the two others wiped and re-added
# with their new addresses.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
hr "S13 setup"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 300 >/dev/null; $WL consume --group g1 --max 60
NEWNS=queen2
NEWFQ() { echo "queen-mq-v2-$1.queen-mq-v2-headless.$NEWNS.svc.cluster.local"; }
NEWPEERS="1=$(NEWFQ 0):7400/$(NEWFQ 0):6632,2=$(NEWFQ 1):7400/$(NEWFQ 1):6632,3=$(NEWFQ 2):7400/$(NEWFQ 2):6632"
start_new() {  # i [env...]: the node on its old volume, reachable ONLY under the new name
  local i="$1"; shift
  $QR start "$i" "QUEEN_RAFT_PEERS=$NEWPEERS" "QUEEN_KAFKA_ADVERTISED_ADDR=$(NEWFQ "$i"):9092" "$@" >/dev/null
  docker network disconnect qr "queen-mq-v2-$i" >/dev/null 2>&1
  docker network connect --alias "$(NEWFQ "$i")" qr "queen-mq-v2-$i"
}
hr "S13 the addresses change (namespace queen -> $NEWNS), data kept"
for i in 0 1 2; do docker stop -t 30 "queen-mq-v2-$i" >/dev/null; done
for i in 0 1 2; do start_new "$i"; done
sleep 15; hs; mem
docker logs queen-mq-v2-0 2>&1 | grep -iE 'unreachable|resolve|dns|failed to lookup' | tail -2 | cut -c1-250

hr "S13 recovery step 1: stop all; node 1 with QUEEN_RAFT_FORCE_RECOVER=1; nodes 2,3 wiped"
for i in 0 1 2; do docker stop -t 30 "queen-mq-v2-$i" >/dev/null; done
$QR wipe 1 >/dev/null; $QR wipe 2 >/dev/null
start_new 0 QUEEN_RAFT_FORCE_RECOVER=1
wait_healthy 0 60; mem
hr "S13 step 2: node 1 without the variable; nodes 2,3 empty; add + promote with NEW addresses"
start_new 0; wait_healthy 0 60
start_new 1; start_new 2; sleep 5
for i in 1 2; do api 0 POST /api/v1/system/raft/membership/learners "{\"id\":$((i+1)),\"raft\":\"$(NEWFQ "$i"):7400\",\"http\":\"$(NEWFQ "$i"):6632\"}" | cut -c1-120; done
for t in $(seq 1 30); do r=$(api 0 POST /api/v1/system/raft/membership/promote '{"ids":[2,3]}'); echo "$r" | grep -q '"ok":true' && break; sleep 2; done
echo "$r" | cut -c1-120
wait_caught_up 1 120; wait_caught_up 2 120; hs; mem
curl -s localhost:16630/api/v1/system/raft/membership | python3 -c 'import sys,json; [print(m["nodeId"], m["raft"]) for m in json.load(sys.stdin)["membership"]["members"]]'
hr "S13 verify"
$WL verify | cut -c1-300
