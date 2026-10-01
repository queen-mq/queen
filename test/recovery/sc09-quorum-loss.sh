#!/usr/bin/env bash
# S9  — two of three nodes down for a while (crash, node drain, zone outage):
#       no quorum, everything answers 503; they come back, it heals by itself.
# S10 — every node killed at once (power loss of the whole cluster, OOM storm):
#       start them again, it heals by itself.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
hr "S9 setup"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 300 >/dev/null; $WL consume --group g1 --max 60
$WL load --seconds 90 --rate 30 --tag L9 > runs/s9-load.log 2>&1 &
LOAD=$!; sleep 5
L=$(leader); a=$L; b=$(( (L + 1) % 3 )); c=$(( (L + 2) % 3 ))
hr "S9 kill the leader and one follower, keep them down 30 s"
$QR kill "$a"; $QR kill "$b"; sleep 5; hs
say "survivor /health: $(h "$c" | cut -c1-250)"
st=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "localhost:1663$c/api/v1/kv/rec/A-k1")
say "a read through the survivor answers HTTP $st"
sleep 25
hr "S9 bring ONE back: quorum again"
t0=$(date +%s); docker start "queen-mq-v2-$a" >/dev/null
for i in $(seq 1 60); do sleep 1; [ -n "$(leader)" ] && break; done
say "leader again after $(( $(date +%s) - t0 ))s"; hs
docker start "queen-mq-v2-$b" >/dev/null; wait_all_healthy 3 120; hs
wait $LOAD; tail -1 runs/s9-load.log
$WL verify | cut -c1-300

hr "S10 kill -9 all three at the same instant, then start them"
$WL load --seconds 40 --rate 30 --tag L10 > runs/s10-load.log 2>&1 &
LOAD=$!; sleep 5
docker kill -s KILL queen-mq-v2-0 queen-mq-v2-1 queen-mq-v2-2 >/dev/null; sleep 3; hs
t0=$(date +%s); for i in 0 1 2; do docker start "queen-mq-v2-$i" >/dev/null; done
wait_all_healthy 3 120; say "all healthy $(( $(date +%s) - t0 ))s after the start"; hs
wait $LOAD; tail -1 runs/s10-load.log
hr "S9/S10 verify"
$WL verify | cut -c1-400
