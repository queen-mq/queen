#!/usr/bin/env bash
# S2 — a follower is down longer than the leader keeps the log for it: when it
# comes back it gets a SNAPSHOT, exits 75 to load it, and its restart policy
# (kubelet) starts it again. Purge hold shortened to 20 s to make it quick.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
export QR_ENV="QUEEN_RAFT_PURGE_HOLD_S=20 QUEEN_RAFT_LOG_KEEP=200"
hr "S2 setup (purge hold 20 s, log keep 200)"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 200; $WL consume --group g1 --max 60
L=$(leader); F=$(( (L + 1) % 3 )); say "leader index $L, victim follower index $F"
hr "S2 stop the follower and keep writing"
$QR stop "$F"
$WL load --seconds 70 --rate 50 --tag L2 > runs/s2-load.log 2>&1
tail -1 runs/s2-load.log; hs
docker logs "queen-mq-v2-$L" 2>&1 | grep -iE 'purg|snapshot' | tail -4 | cut -c1-300
hr "S2 start the follower again"
t0=$(date +%s); docker start "queen-mq-v2-$F" >/dev/null
for i in $(seq 1 60); do sleep 2; s=$(cstate "$F"); case "$s" in *restarts=[1-9]*) break;; esac; done
say "container after $(( $(date +%s) - t0 ))s: $(cstate "$F")"
docker logs "queen-mq-v2-$F" 2>&1 | grep -iE 'snapshot|exits to load' | tail -6 | cut -c1-300
wait_healthy "$F" 180; wait_caught_up "$F" 180; say "follower usable after $(( $(date +%s) - t0 ))s; $(cstate "$F")"
hr "S2 verify (also reading through the restored follower only)"
$WL verify; $WL verify --nodes "$F"; hs
