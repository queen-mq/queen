#!/usr/bin/env bash
# Snapshot Postgres ground-truth counters for the fusion-hysteresis experiment.
# Called twice per load step: LABEL=pre at the warmup boundary, LABEL=post at the
# end of the measure window. analyze.mjs diffs pre/post to compute commits/s,
# msgs/commit, WAL fsync/s, WAL bytes/msg, etc.
#
# Usage:
#   OUT=/path/pre.json  PG_CONTAINER=queen-fusion-pg  ./snapshot-pg.sh pre
#
# The whole snapshot is a single JSON object emitted by psql, so there is exactly
# one round-trip and the pre/post timestamps come from the DB clock (consistent
# with the counters).
set -euo pipefail

LABEL="${1:?label (pre|post) required}"
OUT="${OUT:?OUT (output file path) required}"
PG_CONTAINER="${PG_CONTAINER:-queen-fusion-pg}"
PG_DB="${PG_DB:-queen}"
PG_USER="${PG_USER:-postgres}"

# pg_stat_database  -> xact_commit (commits), tup_inserted (rows), I/O timing
# pg_stat_wal       -> wal_records / wal_bytes / wal_sync (THE fsync amortization signal)
# queen.messages    -> n_tup_ins = pushes (append-only); the clean per-message denominator
SQL=$(cat <<'SQL'
WITH
db AS (
  SELECT jsonb_build_object(
    'ts', extract(epoch from clock_timestamp()),
    'numbackends', numbackends,
    'xact_commit', xact_commit,
    'xact_rollback', xact_rollback,
    'tup_inserted', tup_inserted,
    'tup_updated', tup_updated,
    'tup_deleted', tup_deleted,
    'blks_read', blks_read,
    'blks_hit', blks_hit,
    'blk_read_time', blk_read_time,
    'blk_write_time', blk_write_time,
    'deadlocks', deadlocks
  ) AS j
  FROM pg_stat_database WHERE datname = current_database()
),
wal AS (
  SELECT jsonb_build_object(
    'wal_records', wal_records,
    'wal_fpi', wal_fpi,
    'wal_bytes', wal_bytes,
    'wal_buffers_full', wal_buffers_full,
    'wal_write', wal_write,
    'wal_sync', wal_sync,
    'wal_write_time', wal_write_time,
    'wal_sync_time', wal_sync_time
  ) AS j
  FROM pg_stat_wal
),
msg AS (
  SELECT jsonb_build_object(
    'n_tup_ins', COALESCE(n_tup_ins, 0),
    'n_live_tup', COALESCE(n_live_tup, 0),
    'n_dead_tup', COALESCE(n_dead_tup, 0)
  ) AS j
  FROM pg_stat_user_tables WHERE schemaname = 'queen' AND relname = 'messages'
),
counts AS (
  SELECT jsonb_build_object(
    'messages_total', (SELECT count(*)::bigint FROM queen.messages),
    'partitions_total', (SELECT count(*)::bigint FROM queen.partitions)
  ) AS j
)
SELECT jsonb_build_object(
  'database', (SELECT j FROM db),
  'wal',      (SELECT j FROM wal),
  'messages', COALESCE((SELECT j FROM msg), '{}'::jsonb),
  'row_counts', (SELECT j FROM counts)
)::text;
SQL
)

RAW="$(docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 -U "$PG_USER" -d "$PG_DB" -tAX -c "$SQL")"

node -e "
const fs = require('fs');
const inner = JSON.parse(process.argv[1].trim());
fs.writeFileSync(process.argv[2], JSON.stringify(Object.assign({ label: process.argv[3] }, inner), null, 2));
" "$RAW" "$OUT" "$LABEL"

echo "[snapshot-pg] $LABEL -> $OUT"
