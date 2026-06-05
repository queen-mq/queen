#!/bin/bash
# BROKER: repeatedly enforce completed_retention=300 on the queue, beating the
# producer's startup configure() which otherwise coerces it back to 1800.
Q="${1:-bench-long}"
for i in $(seq 1 30); do
  docker exec postgres psql -U postgres -d postgres -c \
    "UPDATE queen.queues SET completed_retention_seconds=300, retention_enabled=true, retention_seconds=7200 WHERE name='$Q'" >/dev/null 2>&1
  sleep 5
done
echo "[$(date -u +%FT%TZ)] enforce-retention done for $Q"
