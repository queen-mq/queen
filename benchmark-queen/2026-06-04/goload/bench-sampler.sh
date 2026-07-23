#!/usr/bin/env bash
# bench-sampler.sh <outfile> <duration_s>
#
# 1 Hz sampler for the QueenMQ bench host. Every second appends one CSV line:
#   epoch_ms,pg_cpu_pct,pg_mem_mb,queen_cpu_pct,queen_mem_mb,
#   xact_commit_cum,wal_records_cum,wal_bytes_cum,wal_fsyncs_cum,
#   wal_fsync_time_ms_cum,db_size_bytes,active_backends,top_wait
#
# CPU/MEM come from the docker container cgroups (cgroup v2, resolved once at
# start). PG counters come from ONE SELECT round trip per second over a single
# PERSISTENT psql session (docker exec -i held open as a coprocess): this keeps
# per-second cost well under a couple % of one core AND avoids re-spawning a psql
# inside the PG container every second (which would self-inflate pg_cpu_pct).
# Robust to missing values: any unreadable field is emitted empty, never crashes;
# the psql session auto-reconnects if it dies.
#
# CPU% is "percent of one core" (docker-stats semantics: 100% = 1 full core;
# with 32 cores the ceiling is 3200%).
set -u

OUT="${1:?usage: bench-sampler.sh <outfile> <duration_s>}"
DUR="${2:?usage: bench-sampler.sh <outfile> <duration_s>}"

BROKER_CTR="r6682"      # queen broker  -> queen_cpu_pct / queen_mem_mb
PG_CTR="qbench-pg"      # postgres 18   -> pg_cpu_pct / pg_mem_mb + stats
PG_DB="postgres"

# ---- resolve container cgroup paths ONCE ---------------------------------
cg_path() {
  local id
  id=$(docker inspect -f '{{.Id}}' "$1" 2>/dev/null) || return 1
  [ -n "$id" ] || return 1
  printf '/sys/fs/cgroup/system.slice/docker-%s.scope' "$id"
}
BROKER_CG=$(cg_path "$BROKER_CTR" || true)
PG_CG=$(cg_path "$PG_CTR" || true)

read_usec() {  # $1=cgroup dir -> cumulative cpu usage_usec (empty if unreadable)
  [ -n "${1:-}" ] && [ -r "$1/cpu.stat" ] || { printf ''; return; }
  local k v
  while read -r k v; do
    [ "$k" = "usage_usec" ] && { printf '%s' "$v"; return; }
  done < "$1/cpu.stat" 2>/dev/null
}
read_mem() {   # $1=cgroup dir -> memory.current bytes (empty if unreadable)
  [ -n "${1:-}" ] && [ -r "$1/memory.current" ] || { printf ''; return; }
  local v; read -r v < "$1/memory.current" 2>/dev/null && printf '%s' "$v"
}

# One SELECT, one round trip. Sub-selects keep it a single result row.
# PG18 NOTE: pg_stat_wal no longer has wal_write/wal_sync/*_time; WAL fsync
# counters live only in pg_stat_io WHERE object='wal' (fsyncs, fsync_time ms).
PG_SQL="SELECT \
(SELECT xact_commit FROM pg_stat_database WHERE datname='${PG_DB}'),\
(SELECT wal_records FROM pg_stat_wal),\
(SELECT wal_bytes FROM pg_stat_wal),\
(SELECT coalesce(sum(fsyncs),0) FROM pg_stat_io WHERE object='wal'),\
(SELECT coalesce(sum(fsync_time),0) FROM pg_stat_io WHERE object='wal'),\
pg_database_size('${PG_DB}'),\
(SELECT count(*) FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid()),\
(SELECT wait_event FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid() AND wait_event IS NOT NULL GROUP BY wait_event ORDER BY count(*) DESC LIMIT 1);"
PG_EMPTY=",,,,,,,"        # 8 empty fields
PG_SENT="__PGEND__"
PG_OK=0

pg_start() {   # (re)open the persistent psql session
  [ -n "${PGC_PID:-}" ] && kill "$PGC_PID" 2>/dev/null
  coproc PGC { exec docker exec -i "$PG_CTR" psql -U postgres -d "$PG_DB" -qtA -F',' 2>/dev/null; }
  PG_OK=1
}
pg_query() {   # -> 8 comma-separated PG fields (or PG_EMPTY on any failure)
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

# header only when creating a fresh file
if [ ! -s "$OUT" ]; then
  printf 'epoch_ms,pg_cpu_pct,pg_mem_mb,queen_cpu_pct,queen_mem_mb,xact_commit_cum,wal_records_cum,wal_bytes_cum,wal_fsyncs_cum,wal_fsync_time_ms_cum,db_size_bytes,active_backends,top_wait\n' >> "$OUT"
fi

# prime CPU baselines so the FIRST emitted line already has a valid ~1s delta
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

  # cpu % of one core = delta_usec / (dt_ms * 1000) * 100 = delta_usec / (dt*10)
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

# ---- self overhead report (this process + all reaped children) -----------
# NB: read /proc/$$/stat (the script PID) not /proc/self/stat -- inside $(...)
# "self" would resolve to the sed subprocess, reporting ~0.
rest=$(sed 's/.*) //' "/proc/$$/stat" 2>/dev/null)
set -- $rest   # after ")": 1=state ... 12=utime 13=stime 14=cutime 15=cstime
clk=$(getconf CLK_TCK 2>/dev/null); [ -n "$clk" ] || clk=100
awk -v u="${12:-0}" -v s="${13:-0}" -v cu="${14:-0}" -v cs="${15:-0}" -v k="$clk" -v d="$DUR" \
  'BEGIN{sec=(u+s+cu+cs)/k; printf "[bench-sampler] overhead: %.2fs CPU over %ss => %.2f%% of one core\n", sec, d, (d>0?sec/d*100:0)}' >&2
