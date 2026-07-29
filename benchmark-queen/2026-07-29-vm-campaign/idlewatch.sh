#!/usr/bin/env bash
# idlewatch.sh <outdir> <tag> <seconds> [note]
#
# Measures the cell with NO load offered: per-cgroup CPU (same accounting as
# runpt.sh, so an idle core and a loaded core are counted the same way), broker
# and proxy RSS, and the broker's own `sizes:` line. This is the S4 idle-floor
# instrument and the S3 "what does a parked consumer cost when nothing moves"
# instrument.
#
# Emits <outdir>/<tag>.idle.csv (per-second samples) and <outdir>/<tag>.idle.txt
# (the summary: mean cores per component over the dwell, RSS start/end).
set -uo pipefail
OUT=$1; TAG=$2; SECS=${3:-300}; NOTE=${4:-}
mkdir -p "$OUT"
CG=/sys/fs/cgroup
SLICE=queencell.slice
CELLIP=$(cat /root/cell/cellpg.ip)
export PGPASSWORD=postgres

declare -A CGDIR
CGDIR[broker]=$CG/$SLICE/queen-broker.service
CGDIR[proxy]=$CG/$SLICE/queen-proxy.service
for c in cell-pg cell-pxdb; do
  pid=$(docker inspect -f '{{.State.Pid}}' $c 2>/dev/null) || continue
  [ -n "$pid" ] && CGDIR[${c#cell-}]=$CG$(sed 's/^0:://' /proc/$pid/cgroup)
done

usec() { awk '/^usage_usec/{print $2}' "$1/cpu.stat" 2>/dev/null || echo 0; }
rss_kb() { ps -o rss= -p "$1" 2>/dev/null | tr -d ' '; }
BPID=$(systemctl show queen-broker -p MainPID --value)
PPID_=$(systemctl show queen-proxy -p MainPID --value)

QCOUNT=$(psql -h "$CELLIP" -p 5432 -U postgres -d queen -qtAX -c \
  "SELECT (SELECT count(*) FROM queen.queues)||'/'||(SELECT count(*) FROM queen.partitions)" 2>/dev/null)

{
  echo "tag:    $TAG"
  echo "when:   $(date -u +%FT%TZ)"
  echo "note:   $NOTE"
  echo "dwell:  ${SECS}s"
  echo "slice:  cpu.max=$(cat $CG/$SLICE/cpu.max) memory.max=$(cat $CG/$SLICE/memory.max)"
  echo "queues/partitions: $QCOUNT"
  echo "broker pid=$BPID rss_kb_start=$(rss_kb "$BPID")  proxy pid=$PPID_ rss_kb_start=$(rss_kb "$PPID_")"
} >"$OUT/$TAG.idle.txt"

echo "t_unix,comp,usage_usec,rss_kb" >"$OUT/$TAG.idle.csv"
END=$(( $(date +%s) + SECS ))
while [ "$(date +%s)" -lt "$END" ]; do
  now=$(date +%s.%N)
  for k in "${!CGDIR[@]}"; do echo "$now,$k,$(usec "${CGDIR[$k]}"),"; done
  echo "$now,slice,$(usec $CG/$SLICE),"
  echo "$now,broker_rss,0,$(rss_kb "$BPID")"
  echo "$now,proxy_rss,0,$(rss_kb "$PPID_")"
  sleep 2
done >>"$OUT/$TAG.idle.csv"

{
  echo "broker rss_kb_end=$(rss_kb "$BPID")  proxy rss_kb_end=$(rss_kb "$PPID_")"
  echo "queues/partitions after: $(psql -h "$CELLIP" -p 5432 -U postgres -d queen -qtAX -c \
    "SELECT (SELECT count(*) FROM queen.queues)||'/'||(SELECT count(*) FROM queen.partitions)" 2>/dev/null)"
  echo "--- mean cores over the dwell (first->last cumulative sample) ---"
  awk -F, 'NR>1 && $2!~/_rss/ {if(!(($2) in f)){f[$2]=$3; ta[$2]=$1} l[$2]=$3; tb[$2]=$1}
           END{for(c in f){dt=tb[c]-ta[c]; if(dt>0) printf "  %-10s %.4f cores\n", c, (l[c]-f[c])/1e6/dt}}' \
    "$OUT/$TAG.idle.csv" | sort
  echo "--- broker sizes: (last 3 in window) ---"
  grep " sizes: " /root/cell/broker.log | tail -3
} >>"$OUT/$TAG.idle.txt"
cat "$OUT/$TAG.idle.txt"
