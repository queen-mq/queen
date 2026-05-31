#!/usr/bin/env bash
# Samples PG server-side state every INTERVAL seconds into a CSV.
# Works for BOTH stacks (pgmq + Queen) so the headline metric is apples-to-apples.
#
# THE key columns for the talk:
#   active_backends / total_backends -> pgmq pins the PgBouncer pool (~pool size);
#                                       Queen's Vegas controller holds ~2.5 active.
#   n_dead / n_upd / n_del           -> the "Postgres-as-a-queue bloats" story:
#                                       pgmq UPDATEs vt + DELETEs per message;
#                                       Queen's messages table is append-only.
#
# Usage: ./sample-metrics.sh <out.csv> <interval_sec> <container> <schema> <table>
set -u
OUT="${1:-results/metrics.csv}"
INTERVAL="${2:-1}"
CONTAINER="${3:-pgmq-postgres}"
SCHEMA="${4:-pgmq}"
TABLE="${5:-q_bench}"

mkdir -p "$(dirname "$OUT")"
echo "ts,active_backends,total_backends,n_live,n_dead,n_ins,n_upd,n_del,autovacuum_count,rows" > "$OUT"

SQL="SELECT
  extract(epoch from now())::bigint,
  (SELECT count(*) FROM pg_stat_activity WHERE state='active' AND datname='postgres'),
  (SELECT count(*) FROM pg_stat_activity WHERE datname='postgres'),
  COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}' AND relname='${TABLE}'),0),
  COALESCE((SELECT n_dead_tup FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}' AND relname='${TABLE}'),0),
  COALESCE((SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}' AND relname='${TABLE}'),0),
  COALESCE((SELECT n_tup_upd FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}' AND relname='${TABLE}'),0),
  COALESCE((SELECT n_tup_del FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}' AND relname='${TABLE}'),0),
  COALESCE((SELECT autovacuum_count FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}' AND relname='${TABLE}'),0),
  COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname='${SCHEMA}' AND relname='${TABLE}'),0);"

while true; do
  docker exec "$CONTAINER" psql -U postgres -d postgres -t -A -F',' -c "$SQL" >> "$OUT" 2>/dev/null
  sleep "$INTERVAL"
done
