#!/usr/bin/env bash
# S15 — a membership change interrupted between its two steps (joint
# configuration): the leader dies right after "set voters" started. Try to
# provoke it (fresh cluster per attempt), then show what the operator sees and
# the command that finishes it.
set -uo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REC"; rm -rf state
attempt() {
  local delay="$1"
  $QR down >/dev/null; $QR up 3 >/dev/null; wait_all_healthy 3 120 >/dev/null
  $WL write --tag A --msgs 100 --kv 5 >/dev/null
  QR_N=4 $QR start 3 QUEEN_RAFT_JOIN=true >/dev/null; sleep 4
  api 0 POST /api/v1/system/raft/membership/learners "{\"id\":4,\"raft\":\"$(FQ 3):7400\",\"http\":\"$(FQ 3):6632\"}" >/dev/null
  sleep 3
  local L LID OK target
  L=$(leader 4); LID=$((L + 1)); OK=$(( (L + 1) % 3 ))
  target=$(python3 -c "print([v for v in (1,2,3) if v != $LID] + [4])")
  hr "delay ${delay} ms: set voters $target through index $OK, then kill -9 the leader (node $LID)"
  ( api "$OK" PUT /api/v1/system/raft/membership/voters "{\"voters\":$target}" > "runs/s15-put.$delay" 2>&1 & )
  perl -e "select(undef,undef,undef,$delay/1000)"; docker kill -s KILL "queen-mq-v2-$L" >/dev/null
  sleep 6; mem
  cat "runs/s15-put.$delay" | cut -c1-200
}
for d in 0 3 6 10 20 40; do
  attempt "$d"
  if mem | grep -q "joint=\[\["; then say "JOINT CONFIGURATION reached at delay $d ms"; J=1; break; fi
done
hr "S15 what a change answers now, and the finishing command"
api 0 POST /api/v1/system/raft/membership/promote '{"ids":[4]}' | cut -c1-500
if [ -n "${J:-}" ]; then
  last=$(curl -s localhost:16630/api/v1/system/raft/membership | python3 -c 'import sys,json; print(json.load(sys.stdin)["membership"]["joint"][-1])')
  say "finish it: PUT voters $last"
  api 0 PUT /api/v1/system/raft/membership/voters "{\"voters\":$last}" | cut -c1-300
  mem
fi
