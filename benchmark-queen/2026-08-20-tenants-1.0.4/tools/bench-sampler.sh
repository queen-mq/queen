#!/usr/bin/env bash
# bench-sampler.sh <outfile> <duration_s>
#
# 1 Hz sampler for the QueenMQ bench host — 2026-08-20 edition.
#
# Same CSV contract, same fields, same units as the 2026-06-04/07-24 sampler
# (percent of ONE core; 100% = one full core), so the output is directly
# comparable with benchmark-queen/2026-07-24-tenants/raw/*.csv:
#   epoch_ms,pg_cpu_pct,pg_mem_mb,queen_cpu_pct,queen_mem_mb,
#   xact_commit_cum,wal_records_cum,wal_bytes_cum,wal_fsyncs_cum,
#   wal_fsync_time_ms_cum,db_size_bytes,active_backends,top_wait
#
# ONE deviation from the original, deliberate: PostgreSQL runs NATIVE here
# (systemd) rather than in a container, so its cgroup is resolved from the
# postmaster PID via /proc/<pid>/cgroup instead of `docker inspect`. The
# accounting mechanism is identical (cgroup v2 cpu.stat usage_usec covers the
# postmaster and every backend), and native PG avoids the userland docker-proxy
# relay that the 2026-07-29 campaign had to fix as a rig defect. The broker
# still runs in docker (host networking, so it dials PG direct).
#
# psql is likewise native and held open as ONE persistent coprocess session, so
# the per-second cost stays well under a couple % of a core and never
# self-inflates pg_cpu_pct by respawning a client each tick.
set -u

OUT="${1:?usage: bench-sampler.sh <outfile> <duration_s>}"
DUR="${2:?usage: bench-sampler.sh <outfile> <duration_s>}"

BROKER_CTR="${BROKER_CTR:-queen}"
PG_DB="${PG_DB:-queen}"

# ---- resolve cgroup paths ONCE -------------------------------------------
broker_cg() {
  local id
  id=$(docker inspect -f '{{.Id}}' "$BROKER_CTR" 2>/dev/null) || return 1
  [ -n "$id" ] || return 1
  for p in "/sys/fs/cgroup/system.slice/docker-$id.scope" \
           "/sys/fs/cgroup/docker/$id"; do
    [ -r "$p/cpu.stat" ] && { printf '%s' "$p"; return 0; }
  done
  return 1
}
pg_cg() {   # postmaster PID -> its cgroup v2 path
  local pid rel
  pid=$(systemctl show -p MainPID --value postgresql@18-main 2>/dev/null)
  [ -n "$pid" ] && [ "$pid" != "0" ] || pid=$(pgrep -o -f "postgres.*-D /var/lib/postgresql" 2>/dev/null | head -1)
  [ -n "$pid" ] || return 1
  rel=$(awk -F: '$1=="0"{print $3}' "/proc/$pid/cgroup" 2>/dev/null)
  [ -n "$rel" ] || return 1
  [ -r "/sys/fs/cgroup$rel/cpu.stat" ] && { printf '/sys/fs/cgroup%s' "$rel"; return 0; }
  return 1
}
BROKER_CG=$(broker_cg || true)
PG_CG=$(pg_cg || true)
printf '[bench-sampler] broker_cg=%s pg_cg=%s\n' "${BROKER_CG:-NONE}" "${PG_CG:-NONE}" >&2

read_usec() {
  [ -n "${1:-}" ] && [ -r "$1/cpu.stat" ] || { printf ''; return; }
  local k v
  while read -r k v; do
    [ "$k" = "usage_usec" ] && { printf '%s' "$v"; return; }
  done < "$1/cpu.stat" 2>/dev/null
}
read_mem() {
  [ -n "${1:-}" ] && [ -r "$1/memory.current" ] || { printf ''; return; }
  local v; read -r v < "$1/memory.current" 2>/dev/null && printf '%s' "$v"
}

# One SELECT, one round trip. PG18: WAL fsync counters live in pg_stat_io.
PG_SQL="SELECT \
(SELECT xact_commit FROM pg_stat_database WHERE datname='${PG_DB}'),\
(SELECT wal_records FROM pg_stat_wal),\
(SELECT wal_bytes FROM pg_stat_wal),\
(SELECT coalesce(sum(fsyncs),0) FROM pg_stat_io WHERE object='wal'),\
(SELECT coalesce(sum(fsync_time),0) FROM pg_stat_io WHERE object='wal'),\
pg_database_size('${PG_DB}'),\
(SELECT count(*) FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid()),\
(SELECT wait_event FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid() AND wait_event IS NOT NULL GROUP BY wait_event ORDER BY count(*) DESC LIMIT 1);"
PG_EMPTY=",,,,,,,"
PG_SENT="__PGEND__"
PG_OK=0

pg_start() {
  [ -n "${PGC_PID:-}" ] && kill "$PGC_PID" 2>/dev/null
  coproc PGC { exec psql -h 127.0.0.1 -U postgres -d "$PG_DB" -qtA -F',' 2>/dev/null; }
  PG_OK=1
}
pg_query() {
  if [ "$PG_OK" -ne 1 ] || ! kill -0 "${PGC_PID:-0}" 2>/dev/null; then pg_start; fi
  if ! printf '%s\n\\echo %s\n' "$PG_SQL" "$PG_SENT" >&"${PGC[1]}" 2>/dev/null; then
    PG_OK=0; printf '%s' "$PG_EMPTY"; return
  fi
  local line data="" got=0
  while IFS= read -r -t 4 -u "${PGC[0]}" line; do
    [ "$line" = "$PG_SENT" ] && { got=1; break; }
    [ -n "$line" ] && data="$line"
  done
  if [ "$got" -ne 1 ]; then
    PG_OK=0; kill "${PGC_PID:-0}" 2>/dev/null; printf '%s' "$PG_EMPTY"; return
  fi
  [ -z "$data" ] && data="$PG_EMPTY"
  printf '%s' "$data"
}

now_ms() { date +%s%3N; }

if [ ! -s "$OUT" ]; then
  printf 'epoch_ms,pg_cpu_pct,pg_mem_mb,queen_cpu_pct,queen_mem_mb,xact_commit_cum,wal_records_cum,wal_bytes_cum,wal_fsyncs_cum,wal_fsync_time_ms_cum,db_size_bytes,active_backends,top_wait\n' >> "$OUT"
fi

prev_b=$(read_usec "$BROKER_CG"); prev_p=$(read_usec "$PG_CG")
prev_ms=$(now_ms)
start_ms=$prev_ms

i=0
while [ "$i" -lt "$DUR" ]; do
  i=$((i+1))
  target=$(( start_ms + i*1000 ))
  cur=$(now_ms)
  slp=$(( target - cur ))
  if [ "$slp" -gt 0 ]; then
    sleep "$((slp/1000)).$(printf '%03d' $((slp%1000)))"
  fi

  ts=$(now_ms)
  b_usec=$(read_usec "$BROKER_CG"); b_mem=$(read_mem "$BROKER_CG")
  p_usec=$(read_usec "$PG_CG");     p_mem=$(read_mem "$PG_CG")
  dt=$(( ts - prev_ms )); [ "$dt" -le 0 ] && dt=1

  pg_cpu=""; qn_cpu=""
  if [ -n "$p_usec" ] && [ -n "$prev_p" ]; then
    pg_cpu=$(awk -v d=$(( p_usec - prev_p )) -v t="$dt" 'BEGIN{if(d<0||t<=0){print"";exit} printf "%.1f", d/(t*10)}')
  fi
  if [ -n "$b_usec" ] && [ -n "$prev_b" ]; then
    qn_cpu=$(awk -v d=$(( b_usec - prev_b )) -v t="$dt" 'BEGIN{if(d<0||t<=0){print"";exit} printf "%.1f", d/(t*10)}')
  fi
  pg_mem=""; [ -n "$p_mem" ] && pg_mem=$(awk -v b="$p_mem" 'BEGIN{printf "%.1f", b/1048576}')
  qn_mem=""; [ -n "$b_mem" ] && qn_mem=$(awk -v b="$b_mem" 'BEGIN{printf "%.1f", b/1048576}')

  pg=$(pg_query)

  printf '%s,%s,%s,%s,%s,%s\n' "$ts" "$pg_cpu" "$pg_mem" "$qn_cpu" "$qn_mem" "$pg" >> "$OUT"

  prev_b=$b_usec; prev_p=$p_usec; prev_ms=$ts
done

[ -n "${PGC_PID:-}" ] && kill "$PGC_PID" 2>/dev/null

rest=$(sed 's/.*) //' "/proc/$$/stat" 2>/dev/null)
set -- $rest
clk=$(getconf CLK_TCK 2>/dev/null); [ -n "$clk" ] || clk=100
awk -v u="${12:-0}" -v s="${13:-0}" -v cu="${14:-0}" -v cs="${15:-0}" -v k="$clk" -v d="$DUR" \
  'BEGIN{sec=(u+s+cu+cs)/k; printf "[bench-sampler] overhead: %.2fs CPU over %ss => %.2f%% of one core\n", sec, d, (d>0?sec/d*100:0)}' >&2
