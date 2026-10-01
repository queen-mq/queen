#!/usr/bin/env bash
# K3 (Kubernetes) — poisoned entry: every pod stops on one entry. The runbook's
# kubectl steps: OnDelete, QUEEN_RAFT_APPLY_SKIP in the template, delete all pods.
set -uo pipefail
. "$(dirname "$0")/k8s-lib.sh"
cd "$REC"; rm -rf state
hr "K3 setup: arm the injected bug on every pod (test knob), rolling"
k set env sts/queen-mq-v2 QUEEN_TEST_REFUSE_APPLY_KV=rec/poison
k rollout status sts/queen-mq-v2 --timeout=300s >/dev/null; pods
$WL write --tag A --msgs 200 >/dev/null; $WL consume --group g1 --max 60
hr "K3 the poisoned write"
q queen-mq-v2-0 /api/v1/kv/rec/poison -X PUT -H 'Content-Type: application/json' -d '{"value":1,"forever":true}' | cut -c1-200
sleep 8; pods
for i in 0 1 2; do printf '  pod %s: %s\n' "$i" "$(q queen-mq-v2-$i /health | cut -c1-120)"; done
V=$(q queen-mq-v2-0 /health | python3 -c 'import sys,json; f=json.load(sys.stdin)["raft"]["apply"]["failure"]; print("%s:%s"%(f["index"],f["digest"]))')
say "every pod says: set QUEEN_RAFT_APPLY_SKIP=$V"
k logs queen-mq-v2-1 -c queen-mq-v2 | grep -o 'set QUEEN_RAFT_APPLY_SKIP=[^ ]* on EVERY node' | head -1
t0=$(date +%s)
hr "K3 fix: OnDelete, the skip in the template, delete every pod"
k patch sts queen-mq-v2 -p '{"spec":{"updateStrategy":{"type":"OnDelete","rollingUpdate":null}}}'
k set env sts/queen-mq-v2 QUEEN_RAFT_APPLY_SKIP=$V
k delete pod -l run=queen-mq-v2
for i in 0 1 2; do wait_ready "queen-mq-v2-$i" 180; done
for i in 0 1 2; do printf '  pod %s apply: %s\n' "$i" "$(q queen-mq-v2-$i /health | python3 -c 'import sys,json; print(json.load(sys.stdin)["raft"]["apply"])')"; done
say "back in service after $(( $(date +%s) - t0 ))s"
hr "K3 later: drop the skip (and the test knob) and go back to RollingUpdate"
k set env sts/queen-mq-v2 QUEEN_RAFT_APPLY_SKIP- QUEEN_TEST_REFUSE_APPLY_KV-
k patch sts queen-mq-v2 -p '{"spec":{"updateStrategy":{"type":"RollingUpdate"}}}'
k rollout status sts/queen-mq-v2 --timeout=400s | tail -1; pods
hr "K3 verify"
$WL write --tag B --msgs 100 >/dev/null; $WL verify | cut -c1-300
