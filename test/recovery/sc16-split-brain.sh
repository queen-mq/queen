#!/usr/bin/env bash
# S16 — OPERATOR ERROR: QUEEN_RAFT_FORCE_RECOVER run on TWO nodes (each thought it
# was the only survivor). Two independent single-voter clusters now accept
# writes. How to see it, and how to converge on one of them.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
hr "S16 setup"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 300 >/dev/null; $WL consume --group g1 --max 60
hr "S16 the mistake: node 1 AND node 2 started with FORCE_RECOVER naming themselves"
for i in 0 1 2; do docker stop -t 30 "queen-mq-v2-$i" >/dev/null; done
$QR start 0 QUEEN_RAFT_FORCE_RECOVER=1 >/dev/null
$QR start 1 QUEEN_RAFT_FORCE_RECOVER=2 >/dev/null
$QR start 2 >/dev/null
sleep 12; hs
for i in 0 1 2; do
  say "index $i members view: $(curl -s -m 3 localhost:1663$i/api/v1/raft/members | python3 -c 'import sys,json; d=json.load(sys.stdin); print("cluster",d["clusterId"],"leader",d["leaderId"],"term",d["term"],"voters",d["voters"])' 2>/dev/null)"
done
for i in 0 1 2; do say "index $i membership: $(curl -s -m 3 localhost:1663$i/api/v1/system/raft/membership | python3 -c 'import sys,json; m=json.load(sys.stdin)["membership"]; print("leader",m["leader"],"voters",m["voters"],"term",m["term"])' 2>/dev/null)"; done
say "both accept writes:"
$WL write --tag ONE --msgs 50 --kv 0 --queues 1 --nodes 0 | cut -c1-150
$WL write --tag TWO --msgs 50 --kv 0 --queues 1 --nodes 1 | cut -c1-150
say "the same key, two different values:"
curl -s -X PUT localhost:16630/api/v1/kv/rec/split -H 'Content-Type: application/json' -d '{"value":"from-node-1","forever":true}' >/dev/null
curl -s -X PUT localhost:16631/api/v1/kv/rec/split -H 'Content-Type: application/json' -d '{"value":"from-node-2","forever":true}' >/dev/null
say "node 1 says $(curl -s localhost:16630/api/v1/kv/rec/split | cut -c1-80) | node 2 says $(curl -s localhost:16631/api/v1/kv/rec/split | cut -c1-80)"

hr "S16 converge on node 1: stop the others, wipe them, re-add"
docker stop -t 30 queen-mq-v2-1 queen-mq-v2-2 >/dev/null
$QR start 0 >/dev/null; wait_healthy 0 60     # node 1 without the variable
$QR wipe 1 >/dev/null; $QR wipe 2 >/dev/null; $QR start 1 >/dev/null; $QR start 2 >/dev/null; sleep 5
for i in 1 2; do api 0 POST /api/v1/system/raft/membership/learners "{\"id\":$((i+1)),\"raft\":\"$(FQ "$i"):7400\",\"http\":\"$(FQ "$i"):6632\"}" | cut -c1-100; done
for t in $(seq 1 30); do r=$(api 0 POST /api/v1/system/raft/membership/promote '{"ids":[2,3]}'); echo "$r" | grep -q '"ok":true' && break; sleep 2; done
wait_caught_up 1 120; wait_caught_up 2 120; hs; mem
say "the key now: $(curl -s localhost:16632/api/v1/kv/rec/split | cut -c1-120)"
hr "S16 verify: everything before the split + what node 1 took; node 2's writes are gone"
$WL verify --allow-loss | cut -c1-500
