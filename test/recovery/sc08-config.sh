#!/usr/bin/env bash
# S8 — a node with the wrong configuration:
#   a) a different QUEEN_RAFT_TOKEN (secret changed on one pod, rotated half-way)
#   b) QUEEN_RAFT_PEERS that does not list the node itself
#   c) the raft token rotated on every node, one at a time (rolling)
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
hr "S8 setup"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 200 >/dev/null
L=$(leader); V=$(( (L + 1) % 3 ))
hr "S8a node index $V restarted with a WRONG raft token"
$QR start "$V" QUEEN_RAFT_TOKEN=wrong-token >/dev/null; sleep 10; hs; mem
say "its log:"; docker logs "queen-mq-v2-$V" 2>&1 | grep -iE '401|token|unauthor' | tail -3 | cut -c1-300
say "the leader's log:"; docker logs "queen-mq-v2-$L" 2>&1 | grep -iE '401|token|unauthor' | tail -2 | cut -c1-300
$WL write --tag B --msgs 100 --nodes "$L" | cut -c1-200
hr "S8a fix: the right token, restart"
$QR start "$V" >/dev/null; wait_healthy "$V" 60; wait_caught_up "$V" 60; hs
hr "S8b node index $V with QUEEN_RAFT_PEERS missing itself"
bad=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "queen-mq-v2-$V" | grep '^QUEEN_RAFT_PEERS=' | cut -d= -f2- | tr ',' '\n' | grep -v "^$((V+1))=" | paste -sd, -)
$QR start "$V" "QUEEN_RAFT_PEERS=$bad" >/dev/null; sleep 6; say "index $V: $(cstate "$V")"
docker logs "queen-mq-v2-$V" 2>&1 | grep -E 'FATAL' | tail -1 | cut -c1-300
$QR start "$V" >/dev/null; wait_healthy "$V" 60
hr "S8c rotate QUEEN_RAFT_TOKEN on all nodes, one at a time, under load"
$WL load --seconds 60 --rate 30 --tag L8 > runs/s8-load.log 2>&1 &
LOAD=$!; sleep 3
for i in 0 1 2; do
  $QR start "$i" QUEEN_RAFT_TOKEN=rotated-token-1234 >/dev/null; sleep 8; hs
done
wait_all_healthy 3 60; hs; mem
wait $LOAD; tail -1 runs/s8-load.log
hr "S8 verify"
# the next starts must keep the rotated token
$WL verify | cut -c1-300
