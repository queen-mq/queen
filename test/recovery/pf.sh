#!/usr/bin/env bash
# keep a port-forward to each k3s queen pod (localhost:1663i -> pod i:6632), restarting it when the pod restarts
cd "$(dirname "$0")"
for i in 0 1 2; do
  ( while true; do ./kk -n queen port-forward "pod/queen-mq-v2-$i" "1663$i:6632" >/dev/null 2>&1; sleep 1; done ) &
done
wait
