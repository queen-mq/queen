#!/usr/bin/env bash
# down.sh — remove the Kafka rehearsal containers and the kbench network (no volumes are created, none left behind)
docker rm -f kb1 kb2 kb3 kl1 2>/dev/null
docker network rm kbench 2>/dev/null
docker ps -a --filter name='^k[bl][0-9]$' --format '{{.Names}}' | grep . && echo "LEFT OVER" || echo "kafka rehearsal: nothing left"
