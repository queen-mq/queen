# k8s-lib.sh — the runbook's kubectl helpers, pointed at the throwaway k3s.
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
KK="$REC/kk"
k() { "$KK" -n queen "$@"; }
# q POD PATH [curl args...]: curl the broker port INSIDE pod POD (the image has curl)
q() { local p="$1" path="$2"; shift 2; k exec "$p" -c queen-mq-v2 -- curl -s -m 30 "$@" "localhost:6632$path"; echo; }
pods() { k get pods -l run=queen-mq-v2 -o wide --no-headers | awk '{print $1, $2, $3, "restarts="$4, $7}'; }
FQK() { echo "queen-mq-v2-$1.queen-mq-v2-headless.queen.svc.cluster.local"; }
kleader() {  # ordinal of the pod that leads
  local i
  for i in 0 1 2; do
    k exec "queen-mq-v2-$i" -c queen-mq-v2 -- curl -s -m 3 localhost:6632/health 2>/dev/null | grep -q '"role":"leader"' && { echo "$i"; return; }
  done
}
kmem() { local p="${1:-queen-mq-v2-0}"; k exec "$p" -c queen-mq-v2 -- curl -s -m 5 localhost:6632/api/v1/system/raft/membership 2>/dev/null | python3 -c '
import sys,json
m=json.load(sys.stdin)["membership"]
print("leader=%s term=%s voters=%s learners=%s | %s"%(m.get("leader"),m.get("term"),m.get("voters"),m.get("learners")," ".join("%s%s(lag=%s,live=%s)"%(x["nodeId"],"v" if x["voter"] else "L",x.get("lag"),x.get("live")) for x in m["members"])))' 2>/dev/null || echo "(no membership from $p)"; }
wait_ready() {  # pod, seconds
  k wait --for=condition=Ready "pod/$1" --timeout="${2:-180}s" >/dev/null 2>&1 && say "$1 Ready" || say "$1 NOT Ready after ${2:-180}s"
}
