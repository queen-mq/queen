#!/usr/bin/env bash
# store/data.mdb deleted on a follower whose raft log has been PURGED (normal in production)
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
export QR_ENV="QUEEN_RAFT_PURGE_HOLD_S=20 QUEEN_RAFT_LOG_KEEP=200"
$QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120 >/dev/null
$WL write --tag A --msgs 1500 --kv 20 >/dev/null; $WL load --seconds 40 --rate 60 --tag P >/dev/null
L=$(leader); V=$(( (L + 1) % 3 )); OK=$(( (V + 1) % 3 ))
say "purged index on the leader: $(curl -s localhost:1663$L/api/v1/raft/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print({k:d.get(k) for k in ["purgedIndex","appliedIndex","lastLogIndex","snapshotIndex"]})')"
$QR stop "$V" >/dev/null
$QR sh "$V" 'cd /var/lib/queen/raft && cat raft/state.json; echo; rm -v store/data.mdb store/lock.mdb'
docker start "queen-mq-v2-$V" >/dev/null; sleep 15
say "victim: $(cstate "$V") | $(h "$V" | cut -c1-200)"
docker logs "queen-mq-v2-$V" 2>&1 | grep -E 'FATAL|"level":"ERROR"|snapshot' | grep -v registry | tail -4 | cut -c1-300
wait_caught_up "$V" 60
$WL verify --nodes "$V" --allow-loss | cut -c1-300
