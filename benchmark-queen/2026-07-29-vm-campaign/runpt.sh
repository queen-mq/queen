#!/usr/bin/env bash
# runpt.sh — one measured load point.
#
#   runpt.sh <outdir> <run-id> -- <goload args...>
#
# Wraps a goload run with the accounting the campaign requires:
#   * per-cgroup CPU for every cell component (cell PG, pxdb, broker, proxy) AND
#     for the load generator, sampled every second so CPU can be computed over
#     the steady-load window only instead of being diluted by configure/drain
#   * pg_stat_database xact_commit deltas, sampled per second (this is where
#     commits-per-delivered-message comes from — B3)
#   * pg_stat_activity wait events sampled during load (B2's bottleneck question)
#
# The samplers talk to PG over the bridge from the HOST (psql), never through
# `docker exec`: a docker exec would fork the sampler's own psql INSIDE the cell
# cgroup and bill its CPU to Postgres.
#
# Artifacts: <outdir>/<run-id>.{stdout,shape.txt,cpu.csv,pg.csv} plus the
# loader's own <run-id>.json / <run-id>-interval.csv.
set -uo pipefail

OUT=$1; RUNID=$2; shift 2
[ "${1:-}" = "--" ] && shift
mkdir -p "$OUT"

GOLOAD=/root/queen/benchmark-queen/2026-07-29-vm-campaign/goload/goload
SLICE=queencell.slice
CG=/sys/fs/cgroup
CELLIP=$(cat /root/cell/cellpg.ip)
export PGPASSWORD=postgres
PSQL=(psql -h "$CELLIP" -p 5432 -U postgres -d queen -qtAX -v ON_ERROR_STOP=0)

# cgroup dirs, resolved fresh (docker scope names change on every cell restart)
declare -A CGDIR
CGDIR[broker]=$CG/$SLICE/queen-broker.service
CGDIR[proxy]=$CG/$SLICE/queen-proxy.service
for c in cell-pg cell-pxdb; do
  pid=$(docker inspect -f '{{.State.Pid}}' $c 2>/dev/null) || continue
  [ -n "$pid" ] && CGDIR[${c#cell-}]=$CG$(sed 's/^0:://' /proc/$pid/cgroup)
done
CGDIR[dockersvc]=$CG/system.slice/docker.service

usec() { awk '/^usage_usec/{print $2}' "$1/cpu.stat" 2>/dev/null || echo 0; }
# throttled_usec on a capped cgroup is the honest "did we hit the ceiling"
# signal: CPU usage can sit just under the quota while runnable work waits.
thr()  { awk '/^throttled_usec/{print $2}' "$1/cpu.stat" 2>/dev/null || echo 0; }

# ---------------------------------------------------------------- shape
{
  echo "run-id: $RUNID"
  echo "when:   $(date -u +%FT%TZ)"
  echo "cmd:    $GOLOAD $*"
  echo "slice:  cpu.max=$(cat $CG/$SLICE/cpu.max 2>/dev/null) memory.max=$(cat $CG/$SLICE/memory.max 2>/dev/null)"
  echo "health: $(curl -s localhost:6711/healthz)"
  echo "members:"
  for k in "${!CGDIR[@]}"; do echo "    $k -> ${CGDIR[$k]}"; done
  echo "loadavg-before: $(cut -d' ' -f1-3 /proc/loadavg)"
} >"$OUT/$RUNID.shape.txt"

# ---------------------------------------------------------------- samplers
(
  echo "t_unix,comp,usage_usec"
  while :; do
    now=$(date +%s.%N)
    for k in "${!CGDIR[@]}"; do echo "$now,$k,$(usec "${CGDIR[$k]}")"; done
    echo "$now,slice,$(usec $CG/$SLICE)"
    echo "$now,slice_thr,$(thr $CG/$SLICE)"
    lu=0
    for p in $(pgrep -x goload 2>/dev/null); do
      read -r -a s < /proc/$p/stat 2>/dev/null || continue
      lu=$(( lu + ( ${s[13]} + ${s[14]} ) * 10000 ))
    done
    echo "$now,loader,$lu"
    sleep 1
  done
) >"$OUT/$RUNID.cpu.csv" &
CPUPID=$!

# wait events + xact counters, one connection per second from the host
(
  echo "t_unix,kind,a,b,c,d"
  while :; do
    now=$(date +%s.%N)
    "${PSQL[@]}" -c "
      SELECT 'w,'||coalesce(state,'?')||','||coalesce(wait_event_type,'CPU')||','||coalesce(wait_event,'CPU')||','||count(*)
        FROM pg_stat_activity
       WHERE datname='queen' AND backend_type='client backend' AND pid<>pg_backend_pid()
       GROUP BY state, wait_event_type, wait_event
      UNION ALL
      SELECT 'd,'||xact_commit||','||xact_rollback||','||tup_inserted||','||tup_deleted
        FROM pg_stat_database WHERE datname='queen'" 2>/dev/null |
    while IFS= read -r line; do [ -n "$line" ] && echo "$now,$line"; done
    sleep 1
  done
) >"$OUT/$RUNID.pg.csv" &
PGPID=$!

# ---------------------------------------------------------------- the run
# pg_stat_statements, when the cell was brought up with PGSS=1: dump before and
# after and diff by queryid, so commits can be attributed to push / pop / ack
# instead of inferred. Absent extension = silently skipped.
PGSS_ON=$("${PSQL[@]}" -c "SELECT count(*) FROM pg_extension WHERE extname='pg_stat_statements'" 2>/dev/null)
dump_pgss() { # file
  "${PSQL[@]}" -F',' -c "SELECT queryid, calls, rows, round(total_exec_time::numeric,1),
                                replace(left(query,140), ',', ';')
                           FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname='queen')" \
    >"$1" 2>/dev/null
}
if [ "${PGSS_ON:-0}" = "1" ]; then dump_pgss "$OUT/$RUNID.pgss-before.csv"; fi

WAL0=$("${PSQL[@]}" -c "SELECT pg_current_wal_lsn()::text")
"$GOLOAD" "$@" 2>&1 | tee "$OUT/$RUNID.stdout"
RC=${PIPESTATUS[0]}
WAL1=$("${PSQL[@]}" -c "SELECT pg_current_wal_lsn()::text")
if [ "${PGSS_ON:-0}" = "1" ]; then dump_pgss "$OUT/$RUNID.pgss-after.csv"; fi

kill $CPUPID $PGPID 2>/dev/null; wait $CPUPID $PGPID 2>/dev/null

{
  echo "wal_bytes_whole_run: $("${PSQL[@]}" -c "SELECT pg_wal_lsn_diff('$WAL1','$WAL0')::bigint")"
  echo "exit: $RC"
  echo "loadavg-after: $(cut -d' ' -f1-3 /proc/loadavg)"
} >>"$OUT/$RUNID.shape.txt"

echo "[runpt] $RUNID exit=$RC artifacts in $OUT"
exit $RC
