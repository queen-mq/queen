#!/usr/bin/env bash
# up.sh — the Kafka rehearsal rig (SPEC §8): network kbench 10.201.0.0/24, brokers kb1 .2 / kb2 .3 / kb3 .4 (1100 MB each),
# loader kl1 .5 (800 MB): 4.1 GB of container memory in all. Tarball cache mounted read-only at /cache. Idempotent.
set -u
CACHE_DIR=${CACHE_DIR:-/private/tmp/claude-502/-Users-alice-Work-queen/5b439b2e-f94c-4699-9d71-489a54395651/scratchpad/cache}
docker network inspect kbench > /dev/null 2>&1 || docker network create --subnet 10.201.0.0/24 kbench
run() {  # run <name> <ip> <memory>
  docker inspect "$1" > /dev/null 2>&1 && { echo "$1 exists"; return; }
  # --init: a PID 1 that reaps orphans, like systemd on a droplet (sleep infinity alone leaves exited daemons as zombies)
  docker run -d --init --name "$1" --hostname "$1" --network kbench --ip "$2" --memory "$3" --memory-swap "$3" --cpus 2 \
    --ulimit nofile=1048576:1048576 -v "$CACHE_DIR":/cache:ro benchvm:24.04 > /dev/null && echo "$1 $2 $3 up"
}
run kb1 10.201.0.2 1100m
run kb2 10.201.0.3 1100m
run kb3 10.201.0.4 1100m
run kl1 10.201.0.5 800m
docker ps --filter network=kbench --format '{{.Names}} {{.Status}}'
