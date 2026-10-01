#!/usr/bin/env bash
# S14 — a POISONED ENTRY: apply refuses one committed entry on every node (a
# logic bug), so every node stops at it, and restarting replays it and stops
# again. QUEEN_RAFT_APPLY_SKIP=<index>:<digest> on EVERY node rewrites that
# entry into a no-op and the cluster resumes. The bug is injected with the test
# knob QUEEN_TEST_REFUSE_APPLY_KV=rec/poison (apply refuses any entry writing
# that KV key), kept ON throughout: the skip must work with the bug present.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
BUG="QUEEN_TEST_REFUSE_APPLY_KV=rec/poison"
hr "S14 setup (the bug is armed on every node)"
$QR down >/dev/null; QR_ENV="$BUG" $QR up 3 >/dev/null; wait_all_healthy 3 120
$WL write --tag A --msgs 200 >/dev/null; $WL consume --group g1 --max 60
$WL load --seconds 25 --rate 30 --tag L14 > runs/s14-load.log 2>&1 &
LOAD=$!; sleep 5
hr "S14 the poisoned write"
curl -s -m 15 -X PUT localhost:16630/api/v1/kv/rec/poison -H 'Content-Type: application/json' -d '{"value":{"boom":1},"forever":true}'; echo
sleep 5; hs
for i in 0 1 2; do say "index $i /health: $(h "$i" | cut -c1-600)"; done
say "log of index 0:"; docker logs queen-mq-v2-0 2>&1 | grep -E 'APPLY_SKIP' | tail -2 | cut -c1-400
wait $LOAD; tail -1 runs/s14-load.log
SKIP=$(h 0 | python3 -c 'import sys,json; f=json.load(sys.stdin)["raft"]["apply"]["failure"]; print("%s:%s"%(f["index"],f["digest"]))')
say "the value to set: QUEEN_RAFT_APPLY_SKIP=$SKIP"
hr "S14 restart as-is: they stop on it again"
for i in 0 1 2; do $QR restart "$i" >/dev/null; done; sleep 10; hs

hr "S14 the fix: QUEEN_RAFT_APPLY_SKIP=$SKIP on EVERY node, restart all (k8s: set env on the sts)"
for i in 0 1 2; do docker stop -t 30 "queen-mq-v2-$i" >/dev/null; done
for i in 0 1 2; do QR_ENV="$BUG" $QR start "$i" "QUEEN_RAFT_APPLY_SKIP=$SKIP" >/dev/null; done
wait_all_healthy 3 120; hs
for i in 0 1 2; do say "index $i apply: $(h "$i" | python3 -c 'import sys,json; print(json.load(sys.stdin)["raft"]["apply"])' | cut -c1-300)"; done
docker logs queen-mq-v2-0 2>&1 | grep -E 'REPLACED by a skip marker' | tail -1 | cut -c1-400
say "the poisoned key: $(curl -s localhost:16631/api/v1/kv/rec/poison)"
$WL write --tag B --msgs 100 --kv 5 | cut -c1-200
hr "S14 remove QUEEN_RAFT_APPLY_SKIP again (rolling), the cluster keeps working"
for i in 0 1 2; do QR_ENV="$BUG" $QR start "$i" >/dev/null; wait_healthy "$i" 60; done
hs
hr "S14 verify"
$WL verify | cut -c1-500
