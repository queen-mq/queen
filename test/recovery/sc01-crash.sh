#!/usr/bin/env bash
# S1 — one node crashes (kill -9) or restarts: leader and follower, under load.
# Expected: automatic. The restart policy restarts it, it catches up from the
# log, no acknowledged write is lost.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
hr "S1 setup"
$QR down >/dev/null; $QR up 3; wait_all_healthy 3 120
$WL write --tag A --msgs 200; $WL consume --group g1 --max 60
hs
$WL load --seconds 75 --rate 40 --tag L1 > runs/s1-load.log 2>&1 &
LOAD=$!
sleep 5

hr "S1a kill -9 the LEADER"
L=$(leader); say "leader is node index $L"
t0=$(date +%s); $QR crash "$L"
for i in $(seq 1 30); do sleep 1; nl=$(leader); [ -n "$nl" ] && [ "$nl" != "$L" ] && break; done
say "new leader index $nl after $(( $(date +%s) - t0 ))s"; hs
wait_healthy "$L" 120; wait_caught_up "$L" 120; say "old leader back in $(( $(date +%s) - t0 ))s; container: $(cstate "$L")"; hs

hr "S1b kill -9 a FOLLOWER"
L=$(leader); F=$(( (L + 1) % 3 )); say "follower index $F"
t0=$(date +%s); $QR crash "$F"; sleep 2; hs
wait_healthy "$F" 120; wait_caught_up "$F" 120; say "follower back in $(( $(date +%s) - t0 ))s; $(cstate "$F")"

hr "S1c graceful restart of the LEADER (SIGTERM = kubectl delete pod)"
L=$(leader); say "leader index $L"
t0=$(date +%s); $QR restart "$L"; say "restart returned after $(( $(date +%s) - t0 ))s"; hs
docker logs "queen-mq-v2-$L" 2>&1 | grep -iE 'hand|transfer' | tail -3 | cut -c1-300
wait_healthy "$L" 120; wait_caught_up "$L" 120

wait $LOAD; tail -1 runs/s1-load.log
hr "S1 verify"
$WL verify; hs
