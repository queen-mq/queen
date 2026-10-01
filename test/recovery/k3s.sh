#!/usr/bin/env bash
# k3s.sh — a throwaway Kubernetes cluster made of Docker containers (k3s server +
# 3 agents), with its OWN kubeconfig file, to run the helm_v2 chart and rehearse
# the kubectl side of the recovery procedures. Never touches ~/.kube/config.
#   k3s.sh up | down | load <image> | install | kc <kubectl args...>
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
KDIR="$HERE/k3s"; KC="$KDIR/kubeconfig.yaml"
IMG=rancher/k3s:v1.31.6-k3s1
NET=k3snet
export KUBECONFIG="$KC"
kc() { kubectl --kubeconfig "$KC" "$@"; }
case "${1:-}" in
  up)
    mkdir -p "$KDIR"
    docker network inspect $NET >/dev/null 2>&1 || docker network create $NET >/dev/null
    docker run -d --name k3s-server --hostname k3s-server --privileged --network $NET \
      -e K3S_TOKEN=qrtoken -e K3S_KUBECONFIG_OUTPUT=/output/kubeconfig.yaml -e K3S_KUBECONFIG_MODE=666 \
      -v "$KDIR":/output -p 16443:6443 --tmpfs /run --tmpfs /var/run \
      $IMG server --disable traefik --disable metrics-server --tls-san 127.0.0.1 --node-taint node-role.kubernetes.io/control-plane=true:NoSchedule >/dev/null
    for i in 1 2 3; do
      docker run -d --name k3s-agent-$i --hostname k3s-agent-$i --privileged --network $NET \
        -e K3S_URL=https://k3s-server:6443 -e K3S_TOKEN=qrtoken --tmpfs /run --tmpfs /var/run \
        $IMG agent >/dev/null
    done
    until [ -s "$KC" ]; do sleep 1; done
    sed -i '' 's#https://127.0.0.1:6443#https://127.0.0.1:16443#' "$KC"
    until kc get nodes 2>/dev/null | grep -c ' Ready' | grep -q 4; do sleep 2; done
    # kk / hh: kubectl and helm pinned to THIS kubeconfig (never the default context)
    printf '#!/bin/sh\nexec kubectl --kubeconfig "%s" "$@"\n' "$KC" > "$HERE/kk"
    printf '#!/bin/sh\nexec helm --kubeconfig "%s" "$@"\n' "$KC" > "$HERE/hh"
    chmod +x "$HERE/kk" "$HERE/hh"
    kc get nodes ;;
  down)
    docker rm -f k3s-server k3s-agent-1 k3s-agent-2 k3s-agent-3 >/dev/null 2>&1 || true
    docker network rm $NET >/dev/null 2>&1 || true
    rm -rf "$KDIR" "$HERE/kk" "$HERE/hh"; echo "k3s removed" ;;
  load)
    for i in 1 2 3; do docker save "$2" | docker exec -i k3s-agent-$i ctr images import - >/dev/null; done
    echo "loaded $2 on the 3 agents" ;;
  kc) shift; kc "$@" ;;
  *) sed -n '2,6p' "$0"; exit 2 ;;
esac
