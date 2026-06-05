#!/bin/bash
# BROKER: sample CPU + PG commits + message inserts (for fusion ratio).
N="${1:-95}"
for i in $(seq 1 "$N"); do
  TS=$(date -u +%FT%TZ)
  CPU=$(docker stats --no-stream --format "{{.Name}}={{.CPUPerc}}" queen postgres 2>/dev/null | tr "\n" " ")
  ROW=$(docker exec postgres psql -U postgres -d postgres -tAc \
    "SELECT (SELECT xact_commit FROM pg_stat_database WHERE datname='postgres'), COALESCE((SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0)" 2>/dev/null)
  echo "$TS | $CPU | commit_ins=$ROW"
  sleep 8
done
echo "SAMPLE-DONE"
