#!/usr/bin/env bash
# Per-cell sampler for the engine-scaling sweep.
#
# Every INTERVAL seconds, appends one TSV row capturing the two candidate
# ceilings side by side:
#   - broker side : container CPU (vCPU), mem, and MAX event-loop lag (evl=) seen
#                   in the last INTERVAL of queen.log  -> engine-loop saturation.
#   - postgres side: active backends, xact_commit, messages n_tup_ins/del,
#                   WAL bytes -> DB-path saturation / ground-truth push rate.
#
# Cumulative counters (xact_commit, n_tup_ins/del, wal_bytes) are emitted raw;
# summarize-engine.py turns them into per-second rates via first/last deltas.
#
# Usage: mon-engine.sh <outfile> <interval_sec> [queen_container] [pg_container]
set -u

OUT="${1:?outfile required}"
INTERVAL="${2:-3}"
QUEEN="${3:-queen}"
PG="${4:-postgres}"

# WAL window slightly wider than INTERVAL so we never miss an evl print whose
# cadence we don't control.
EVL_WINDOW=$(( INTERVAL + 2 ))

if [ ! -f "$OUT" ]; then
  printf 'ts_epoch\tbroker_vcpu\tbroker_mem\tpg_vcpu\tpg_active\txact_commit\tn_tup_ins\tn_tup_del\twal_bytes\tevl_max_ms\n' > "$OUT"
fi

pg_query() {
  # Single round-trip, tab-separated. Mirrors long-mon.sh's scalar-subquery style.
  docker exec "$PG" psql -U postgres -d postgres -tAF $'\t' -c \
"SELECT
   (SELECT count(*) FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid()),
   (SELECT xact_commit FROM pg_stat_database WHERE datname='postgres'),
   COALESCE((SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0),
   COALESCE((SELECT n_tup_del FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0),
   pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint" 2>/dev/null
}

while true; do
  TS=$(date +%s)

  # docker stats: "queen=612.34%/123MiB / 1GiB  postgres=1700%/..."
  STATS=$(docker stats --no-stream --format "{{.Name}}={{.CPUPerc}}|{{.MemUsage}}" "$QUEEN" "$PG" 2>/dev/null)
  BROKER_CPU=$(printf '%s\n' "$STATS" | awk -F'[=|%]' -v n="$QUEEN" '$1==n{print $2/100; found=1} END{if(!found)print "NA"}')
  BROKER_MEM=$(printf '%s\n' "$STATS" | awk -F'[=|]'  -v n="$QUEEN" '$1==n{print $3}')
  PG_CPU=$(printf '%s\n'     "$STATS" | awk -F'[=|%]' -v n="$PG"    '$1==n{print $2/100; found=1} END{if(!found)print "NA"}')
  [ -z "${BROKER_CPU:-}" ] && BROKER_CPU="NA"
  [ -z "${BROKER_MEM:-}" ] && BROKER_MEM="NA"
  [ -z "${PG_CPU:-}" ] && PG_CPU="NA"

  ROW=$(pg_query)
  if [ -z "$ROW" ]; then ROW=$'NA\tNA\tNA\tNA\tNA'; fi
  PG_ACTIVE=$(printf '%s' "$ROW" | cut -f1)
  XACT=$(printf '%s' "$ROW" | cut -f2)
  NINS=$(printf '%s' "$ROW" | cut -f3)
  NDEL=$(printf '%s' "$ROW" | cut -f4)
  WAL=$(printf '%s' "$ROW" | cut -f5)

  # Max event-loop lag printed by any engine in the last EVL_WINDOW seconds.
  # The engine prints "... evl=<N>ms" in its periodic stats line (queen.hpp).
  EVL=$(docker logs --since "${EVL_WINDOW}s" "$QUEEN" 2>&1 \
        | grep -oE 'evl=[0-9]+' | sed 's/evl=//' | sort -n | tail -1)
  [ -z "$EVL" ] && EVL="NA"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$TS" "$BROKER_CPU" "$BROKER_MEM" "$PG_CPU" "$PG_ACTIVE" "$XACT" "$NINS" "$NDEL" "$WAL" "$EVL" >> "$OUT"

  sleep "$INTERVAL"
done
