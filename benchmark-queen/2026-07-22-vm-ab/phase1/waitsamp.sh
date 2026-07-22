#!/usr/bin/env bash
# waitsamp.sh OUTFILE COUNT  -- 1s pg_stat_activity active-wait aggregate
OUT="${1:?}"; N="${2:?}"; PG=qbench-pg
for ((i=0;i<N;i++)); do
  T=$(date -u +%s)
  docker exec "$PG" psql -U postgres -tAc \
    "select $T||','||coalesce(wait_event_type,'CPU')||','||coalesce(wait_event,'run')||','||count(*) from pg_stat_activity where state='active' and pid<>pg_backend_pid() group by coalesce(wait_event_type,'CPU'), coalesce(wait_event,'run')" \
    </dev/null 2>/dev/null >> "$OUT"
  sleep 1
done
