#!/usr/bin/env bash
# Soak sampler: per-interval snapshot for endurance/bloat tracking.
# Columns: ts, active_backends, schema_bytes (sum of all tables+indexes in the
# schema), dead_tuples (sum), autovacuum_count (sum), main_ins, main_del,
# main_live  — where `main` is the busiest table (pgmq.q_bench / queen.messages).
#
# Usage: ./sample-soak.sh <out.csv> <interval_sec> <container> <schema> <main_table>
set -u
OUT="${1:-results/soak.csv}"; INTERVAL="${2:-5}"; CONTAINER="${3:-pgmq-postgres}"
SCHEMA="${4:-pgmq}"; MAIN="${5:-q_bench}"
mkdir -p "$(dirname "$OUT")"
echo "ts,active_backends,schema_bytes,dead_tuples,autovacuum_count,main_ins,main_del,main_live" > "$OUT"
SQL="SELECT extract(epoch from now())::bigint,
  (SELECT count(*) FROM pg_stat_activity WHERE state='active' AND datname='postgres'),
  COALESCE((SELECT sum(pg_total_relation_size(relid)) FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}'),0),
  COALESCE((SELECT sum(n_dead_tup) FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}'),0),
  COALESCE((SELECT sum(autovacuum_count) FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}'),0),
  COALESCE((SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}' AND relname='${MAIN}'),0),
  COALESCE((SELECT n_tup_del FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}' AND relname='${MAIN}'),0),
  COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}' AND relname='${MAIN}'),0);"
while true; do
  docker exec "$CONTAINER" psql -U postgres -d postgres -t -A -F',' -c "$SQL" >> "$OUT" 2>/dev/null
  sleep "$INTERVAL"
done
