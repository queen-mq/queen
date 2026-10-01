#!/usr/bin/env bash
# S6 — one node's disk is rolled back to an OLD copy of itself (a VM/volume
# snapshot restored on one node only, a backup copied back) while the others
# kept going. What happens, and what fixes it.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
hr "S6 setup"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 200 >/dev/null; $WL consume --group g1 --max 60
L=$(leader); V=$(( (L + 1) % 3 )); OK=$(( (V + 1) % 3 )); ID=$((V + 1))
say "victim follower index $V (node $ID)"
hr "S6 take a copy of node $ID's disk (stopped), keep the cluster writing"
$QR stop "$V" >/dev/null
docker volume rm qr-old >/dev/null 2>&1; docker volume create qr-old >/dev/null
docker run --rm -v "qr-data-$V":/src -v qr-old:/dst --entrypoint /bin/sh "$IMAGE" -c 'cp -a /src/. /dst/'
docker start "queen-mq-v2-$V" >/dev/null; wait_healthy "$V" 60
$WL write --tag B --msgs 300 >/dev/null; $WL consume --group g1 --max 60
hs
hr "S6 roll node $ID back to the old copy and start it"
$QR stop "$V" >/dev/null
docker run --rm -v qr-old:/src -v "qr-data-$V":/dst --entrypoint /bin/sh "$IMAGE" -c 'rm -rf /dst/* && cp -a /src/. /dst/'
docker start "queen-mq-v2-$V" >/dev/null
for i in $(seq 1 20); do sleep 2; hs; done | tail -4
curl -s "localhost:1663$L/api/v1/raft/members" | python3 -c '
import sys,json
for m in json.load(sys.stdin)["members"]:
    print({k:m.get(k) for k in ["nodeId","state","appliedIndex","lastLogIndex","matchIndex","lagEntries"]})'
$WL verify --nodes "$V" --allow-loss | cut -c1-300
hr "S6 remedy: replace node $ID"
replace_node "$V" "$OK"
hr "S6 verify"
$WL verify | cut -c1-300; $WL verify --nodes "$V" | cut -c1-300; hs
