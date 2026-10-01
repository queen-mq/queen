#!/usr/bin/env bash
# S17 — the leader is cut off the network (minority side of a partition): it
#       must step down, the majority elects, it rejoins when the network heals.
# S22 — QUEEN_ENCRYPTION_KEY changed (secret rotated or replaced by mistake) on
#       every node: what consumers of an encrypted queue get, and the way back.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
hr "S17 setup"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 200 >/dev/null
$WL load --seconds 50 --rate 30 --tag L17 > runs/s17-load.log 2>&1 &
LOAD=$!; sleep 5
L=$(leader); say "isolating the leader index $L for 20 s"
docker network disconnect qr "queen-mq-v2-$L"
for i in $(seq 1 10); do sleep 2; hs; done | sed -n '1p;3p;5p;10p'
say "the isolated old leader says: $(h "$L" | cut -c1-200)"
docker network connect --alias "$(FQ "$L")" qr "queen-mq-v2-$L"
wait_healthy "$L" 60; wait_caught_up "$L" 60; hs
wait $LOAD; tail -1 runs/s17-load.log
$WL verify | cut -c1-300

hr "S22 an encrypted queue, then QUEEN_ENCRYPTION_KEY replaced on every node"
curl -s -X POST localhost:16630/api/v1/configure -H 'Content-Type: application/json' -d '{"queue":"rec.secret","options":{"encryptionEnabled":true}}' | cut -c1-150; echo
curl -s -X POST localhost:16630/api/v1/push -H 'Content-Type: application/json' -d '{"items":[{"queue":"rec.secret","partition":"p","transactionId":"s-1","payload":{"card":"4111"}}]}' | cut -c1-150; echo
say "read with the right key: $(curl -s 'localhost:16631/api/v1/pop/queue/rec.secret?consumerGroup=r1&subscriptionMode=all&autoAck=true' | python3 -c 'import sys,json; print(json.load(sys.stdin)["messages"][0]["data"])')"
NEWKEY=$(openssl rand -hex 32)
for i in 0 1 2; do $QR start "$i" "QUEEN_ENCRYPTION_KEY=$NEWKEY" >/dev/null; wait_healthy "$i" 60 >/dev/null; done
say "read with the WRONG key: $(curl -s 'localhost:16631/api/v1/pop/queue/rec.secret?consumerGroup=r2&subscriptionMode=all&autoAck=true' | python3 -c 'import sys,json; print(json.load(sys.stdin)["messages"][0]["data"])' | cut -c1-200)"
docker logs queen-mq-v2-1 2>&1 | grep -iE 'decrypt|encrypt' | grep -E 'WARN|ERROR' | tail -2 | cut -c1-250
hr "S22 the way back: the original key, rolling restart"
for i in 0 1 2; do $QR start "$i" >/dev/null; wait_healthy "$i" 60 >/dev/null; done
say "read with the right key again: $(curl -s 'localhost:16631/api/v1/pop/queue/rec.secret?consumerGroup=r3&subscriptionMode=all&autoAck=true' | python3 -c 'import sys,json; print(json.load(sys.stdin)["messages"][0]["data"])')"
