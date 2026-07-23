#!/bin/bash
# Usage: samplev2.sh LABEL DURATION_S
# One persistent psql session (\watch every 2s) with forced-fresh stats:
# records epoch, xact_commit (postgres db), and #active backends.
LABEL="$1"
DUR="${2:-100}"
OUT=/root/t2out
mkdir -p "$OUT"
CSV="$OUT/commitv2_${LABEL}.csv"
timeout "$DUR" docker exec -e PGOPTIONS='-c stats_fetch_consistency=none' qbench-pg \
  psql -U postgres -At -F',' -c \
  "SELECT extract(epoch from clock_timestamp())::bigint AS ep, (SELECT xact_commit FROM pg_stat_database WHERE datname='postgres') AS commits, (SELECT count(*) FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid()) AS active_bk \watch 2" \
  > "$CSV" 2>/dev/null
echo "SAMPLERV2_DONE ${LABEL}"
