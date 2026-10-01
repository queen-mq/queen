# lib.sh — helpers for the recovery scenario scripts (source it).
REC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QR="$REC/qr.sh"
WL="python3 $REC/wl.py"
IMAGE="${QR_IMAGE:-local/queen:recovery}"
CHART="${QR_CHART:-$(git -C "$REC" rev-parse --show-toplevel)/helm_v2/broker}"
export PATH="$REC:$PATH"

ts() { date -u +%H:%M:%S; }
say() { echo "[$(ts)] $*"; }
hr() { echo; echo "==================== $* ===================="; }

# node i's /health JSON ('' when it does not answer)
h() { curl -s -m 2 "localhost:1663$1/health" 2>/dev/null; }
# role of node i (leader/follower/learner/candidate/stopped/down)
role() { local j; j=$(h "$1"); [ -z "$j" ] && { echo down; return; }; echo "$j" | python3 -c 'import sys,json; print(json.load(sys.stdin)["raft"]["role"])' 2>/dev/null || echo "?"; }
status() { local j; j=$(h "$1"); [ -z "$j" ] && { echo down; return; }; echo "$j" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])' 2>/dev/null || echo "?"; }
# the index (0-based container) of the leader among nodes 0..N-1, '' when none
leader() {
  local n="${1:-3}" i
  for ((i = 0; i < n; i++)); do [ "$(role "$i")" = leader ] && { echo "$i"; return; }; done
  echo ""
}
# wait until node i answers /health 200, up to s seconds
wait_healthy() {
  local i="$1" s="${2:-120}" t=0
  while [ "$t" -lt "$s" ]; do
    [ "$(status "$i")" = healthy ] && { say "node index $i healthy after ${t}s"; return 0; }
    sleep 1; t=$((t + 1))
  done
  say "node index $i NOT healthy after ${s}s: $(h "$i")"; return 1
}
wait_all_healthy() {
  local n="${1:-3}" s="${2:-180}" i
  for ((i = 0; i < n; i++)); do wait_healthy "$i" "$s" || return 1; done
}
# wait until node i's applied index equals the leader's commit
wait_caught_up() {
  local i="$1" s="${2:-180}" t=0 n="${3:-3}"
  local l lc mi
  while [ "$t" -lt "$s" ]; do
    l=$(leader "$n")
    if [ -n "$l" ]; then
      lc=$(h "$l" | python3 -c 'import sys,json; print(json.load(sys.stdin)["raft"]["commit"])' 2>/dev/null)
      mi=$(h "$i" | python3 -c 'import sys,json; print(json.load(sys.stdin)["raft"]["applied"])' 2>/dev/null)
      if [ -n "$lc" ] && [ -n "$mi" ] && [ "$mi" -ge "$lc" ]; then say "node index $i caught up (applied $mi >= leader commit $lc) after ${t}s"; return 0; fi
    fi
    sleep 1; t=$((t + 1))
  done
  say "node index $i not caught up after ${s}s"; return 1
}
# membership as the leader sees it (compact)
mem() {
  local i
  for i in 0 1 2 3 4; do
    local out; out=$(curl -s -m 3 "localhost:1663$i/api/v1/system/raft/membership" 2>/dev/null) || continue
    [ -z "$out" ] && continue
    echo "$out" | python3 -c '
import sys,json
m=json.load(sys.stdin)["membership"]
ms=" ".join("%s%s(lag=%s,live=%s)"%(x["nodeId"],"v" if x["voter"] else "L",x.get("lag"),x.get("live")) for x in m["members"])
print("leader=%s term=%s voters=%s learners=%s joint=%s src=%s via=%s | %s"%(m.get("leader"),m.get("term"),m.get("voters"),m.get("learners"),m.get("joint"),m.get("source"),m.get("nodeId"),ms))'
    return
  done
  echo "(no node answered membership)"
}
# one-line health summary of nodes 0..n-1
hs() {
  local n="${1:-3}" i out="" j
  for ((i = 0; i < n; i++)); do
    j=$(h "$i")
    if [ -z "$j" ]; then out="$out | $i:down"; continue; fi
    out="$out | $i:$(echo "$j" | python3 -c 'import sys,json; d=json.load(sys.stdin); r=d["raft"]; print("%s/%s t%s a%s c%s lag%s%s"%(d["status"][:4],r["role"][:4],r["term"],r["applied"],r["commit"],r["lag"]," FAIL" if (r.get("apply") or {}).get("failure") else ""))' 2>/dev/null)"
  done
  echo "[$(ts)]$out"
}
# docker state of the container of node i: running/restarting/exited(code)
cstate() { docker inspect -f '{{.State.Status}}({{.State.ExitCode}}) restarts={{.RestartCount}}' "queen-mq-v2-$1" 2>/dev/null || echo absent; }
# the last FATAL/ERROR log lines of node i
why() { docker logs "queen-mq-v2-$1" 2>&1 | grep -E 'FATAL|"level":"ERROR"|panicked' | tail -"${2:-3}" | cut -c1-600; }
# membership API call through node i: api i METHOD PATH [JSON]
api() {
  local i="$1" m="$2" p="$3" b="${4:-}"
  if [ -n "$b" ]; then
    curl -s -m 40 -X "$m" "localhost:1663$i$p" -H 'Content-Type: application/json' -d "$b"
  else
    curl -s -m 40 -X "$m" "localhost:1663$i$p"
  fi
  echo
}
FQ() { echo "queen-mq-v2-$1.queen-mq-v2-headless.queen.svc.cluster.local"; }

# The replace procedure for node index v (id v+1), driven through node index ok:
# remove from the membership, wipe, start empty, add as learner, promote.
replace_node() {
  local v="$1" ok="$2" id=$(( $1 + 1 ))
  say "replace node $id: remove"
  api "$ok" DELETE "/api/v1/system/raft/membership/members/$id" | cut -c1-160
  say "replace node $id: wipe + start empty"
  $QR wipe "$v" >/dev/null; $QR start "$v" >/dev/null
  local t=0; until [ -n "$(h "$v")" ] || [ $t -ge 60 ]; do sleep 1; t=$((t+1)); done
  say "replace node $id: add learner"
  api "$ok" POST /api/v1/system/raft/membership/learners "{\"id\":$id,\"raft\":\"$(FQ "$v"):7400\",\"http\":\"$(FQ "$v"):6632\"}" | cut -c1-160
  local i r
  for i in $(seq 1 60); do
    r=$(api "$ok" POST /api/v1/system/raft/membership/promote "{\"ids\":[$id]}")
    echo "$r" | grep -q '"ok":true' && { say "replace node $id: promoted"; break; }
    sleep 2
  done
  echo "$r" | cut -c1-200
  wait_caught_up "$v" 120
}
