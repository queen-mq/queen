#!/usr/bin/env bash
# fresh queen-mq-v2 on the k3s: uninstall, drop every PVC, install again, wait Ready
set -uo pipefail
. "$(dirname "$0")/k8s-lib.sh"
cd "$REC"
"$QR" secrets >/dev/null; set -a; . "$REC/secrets.env"; set +a
k get ns queen >/dev/null 2>&1 || "$KK" create namespace queen >/dev/null
k get secret queen-mq-v2-prod >/dev/null 2>&1 || k create secret generic queen-mq-v2-prod \
  --from-literal=QUEEN_RAFT_TOKEN="$QUEEN_RAFT_TOKEN" --from-literal=JWT_SECRET="$JWT_SECRET" \
  --from-literal=QUEEN_PROXY_CP_TOKEN="$QUEEN_PROXY_CP_TOKEN" --from-literal=QUEEN_TOKEN="$QUEEN_TOKEN" \
  --from-literal=QUEEN_PROXY_BOOTSTRAP_API_KEY="$QUEEN_PROXY_BOOTSTRAP_API_KEY" >/dev/null
k get secret queen-prod >/dev/null 2>&1 || k create secret generic queen-prod --from-literal=QUEEN_ENCRYPTION_KEY="$QUEEN_ENCRYPTION_KEY" >/dev/null
k get secret queen-proxy-google >/dev/null 2>&1 || k create secret generic queen-proxy-google --from-literal=GOOGLE_CLIENT_ID=dummy --from-literal=GOOGLE_CLIENT_SECRET=dummy >/dev/null
"$REC/hh" uninstall queen-mq-v2 -n queen >/dev/null 2>&1
k wait --for=delete pod -l run=queen-mq-v2 --timeout=180s >/dev/null 2>&1
k delete pvc --all --wait=true >/dev/null 2>&1
"$REC/hh" install queen-mq-v2 "$CHART" -n queen -f "$CHART"/prod.yaml -f "$REC/k3s-values.yaml" ${EXTRA_HELM:-} >/dev/null
k rollout status sts/queen-mq-v2 --timeout=240s >/dev/null && say "fresh queen-mq-v2 ready"
pods
