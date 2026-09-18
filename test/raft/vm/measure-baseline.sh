#!/bin/bash
# WP-0.2 — postgres-class baseline on the Linux VM (PLAN_RAFT.md §13.6, Appendix G).
#
# One broker on :6698 against the VM's local Postgres (5432, user/password
# postgres), fresh schema per lane, goload -mode openloop, plus a CPU/RSS
# sampler (every 5 s, from /proc and the Postgres cgroup) for the queen, the
# postgres and the goload processes (the loader shares the VM: its cores tell
# whether a ceiling is the broker's or the loader's).
#
# usage: measure-baseline.sh <tag> ["<regimes>"] ["<extra broker env>"]
#   regimes: any of A20k A50k B1 C1000 D1 FAT100 (default: all six, in that
#            order — FAT100 last, it leaves tens of GB of messages behind).
# env overrides:
#   QUEEN_BIN   broker binary          (default /root/raft/queen-raft/server/target/release/queen)
#   GOLOAD      loader binary          (default /root/goload)
#   OUTDIR      output directory       (default /root/raft/baseline-<tag>)
#   BROKER_PORT broker HTTP port       (default 6698)
#   FAT_RATE    FAT100 offered msg/s   (default 300000: the highest offered
#               rate this VM sustains with shed=0 and no latency drift —
#               calibrated 2026-09-17, see baseline/RESULTS.md)
#   FAT_DURATION FAT100 seconds        (default 60)
#   SAMPLE_SEC  sampler period         (default 5)
#
# Outputs in OUTDIR: summary.txt, <regime>.log, cpu-rss.csv, marks.csv,
# cpu-summary.txt, broker.log.
set -u

TAG=${1:-baseline}
REGIMES=${2:-"A20k A50k B1 C1000 D1 FAT100"}
EXTRA=${3:-}

BIN=${QUEEN_BIN:-/root/raft/queen-raft/server/target/release/queen}
G=${GOLOAD:-/root/goload}
BROKER_PORT=${BROKER_PORT:-6698}
FAT_RATE=${FAT_RATE:-300000}
FAT_DURATION=${FAT_DURATION:-60}
SAMPLE_SEC=${SAMPLE_SEC:-5}
D=${OUTDIR:-/root/raft/baseline-$TAG}
URL=http://127.0.0.1:$BROKER_PORT

[ -x "$BIN" ] || { echo "no broker binary at $BIN"; exit 1; }
[ -x "$G" ]   || { echo "no goload at $G"; exit 1; }

rm -rf "$D"; mkdir -p "$D"
HZ=$(getconf CLK_TCK); PAGE=$(getconf PAGESIZE)

# ---------------------------------------------------------------- fresh lane
(pkill -x queen 2>/dev/null; true); sleep 1
PGPASSWORD=postgres psql -h localhost -U postgres -qc \
  "DROP SCHEMA IF EXISTS queen CASCADE; DROP SCHEMA IF EXISTS queen_streams CASCADE;" >/dev/null 2>&1

{ echo "host: $(hostname) $(uname -sr) $(nproc) vCPU $(free -g | awk '/^Mem:/{print $2}') GB"
  echo "binary: $BIN"
  echo "binary md5: $(md5sum $BIN | cut -c1-12)"
  echo "loader: $G ($(md5sum $G | cut -c1-12))"
  echo "postgres: $(PGPASSWORD=postgres psql -h localhost -U postgres -tAc 'select version()' | cut -c1-60)"
  echo "regimes: $REGIMES"
  echo "started: $(date -Is)"
} | tee -a "$D/summary.txt"

# ------------------------------------------------------------------- broker
env PG_HOST=localhost PG_PORT=5432 PG_USER=postgres PG_PASSWORD=postgres PG_DATABASE=postgres \
  QUEEN_APPLY_SCHEMA=1 PORT=$BROKER_PORT DB_POOL_SIZE=64 FILE_BUFFER_DIR=$D/spool \
  LOG_LEVEL=warn $EXTRA nohup "$BIN" > "$D/broker.log" 2>&1 &
QPID=$!
echo $QPID > "$D/pid"
for i in $(seq 1 90); do curl -sf $URL/health >/dev/null 2>&1 && break; sleep 1; done
curl -sf $URL/health >/dev/null 2>&1 || { echo "broker did not come up, see $D/broker.log" | tee -a "$D/summary.txt"; kill $QPID 2>/dev/null; exit 1; }
echo "broker pid $QPID up on $BROKER_PORT, version $(curl -s $URL/health | tr ',' '\n' | grep -i version | head -1)" | tee -a "$D/summary.txt"

# ------------------------------------------------------------- CPU/RSS sampler
# Cumulative CPU seconds and memory per role, straight from /proc (and, for
# Postgres, from the service cgroup when it exists, so backends that exit
# mid-run are still counted). Cores are deltas of these columns over the
# regime window; see cpu-summary.txt.
PGCG=/sys/fs/cgroup/system.slice/system-postgresql.slice/postgresql@16-main.service/cpu.stat
echo "ts,role,nproc,cpu_s,rss_kb,pss_kb" > "$D/cpu-rss.csv"
sample_role() { # $1 role, $2 pids
  local role=$1 pids=$2 cpu=0 rss=0 pss=0 n=0 u s r p
  for pid in $pids; do
    [ -r /proc/$pid/stat ] || continue
    read u s r <<< "$(awk '{ sub(/^[0-9]+ \(.*\) /, ""); print $12, $13, $22 }' /proc/$pid/stat 2>/dev/null)"
    [ -n "${u:-}" ] || continue
    cpu=$((cpu + u + s)); rss=$((rss + r)); n=$((n + 1))
    p=$(awk '/^Pss:/{s+=$2} END{print s+0}' /proc/$pid/smaps_rollup 2>/dev/null)
    pss=$((pss + ${p:-0}))
  done
  # a role with no live process (the loader between regimes) emits no row,
  # so a zero never lands at a window edge and skews the delta.
  [ "$n" -gt 0 ] || [ "$role" = postgres ] || return 0
  if [ "$role" = postgres ] && [ -r "$PGCG" ]; then
    cpu=$(awk -v hz=$HZ '/^usage_usec/{printf "%d", $2*hz/1000000}' "$PGCG")
  fi
  awk -v ts=$(date +%s) -v role=$role -v n=$n -v cpu=$cpu -v hz=$HZ -v rss=$rss -v pg=$PAGE -v pss=$pss \
    'BEGIN{printf "%d,%s,%d,%.2f,%d,%d\n", ts, role, n, cpu/hz, rss*pg/1024, pss}' >> "$D/cpu-rss.csv"
}
sampler() {
  while [ ! -f "$D/stop" ]; do
    sample_role queen "$QPID"
    sample_role postgres "$(pgrep -x postgres | tr '\n' ' ')"
    # the loader runs on the same VM: sample it too, so a regime that stops
    # scaling can be attributed to the broker or to the loader.
    sample_role goload "$(pgrep -x "$(basename "$G")" | tr '\n' ' ')"
    sleep "$SAMPLE_SEC"
  done
}
sampler & SPID=$!

# ----------------------------------------------------------------- the lanes
echo "regime,start_ts,end_ts,db_bytes_end" > "$D/marks.csv"
run() {
  local name=$1; shift
  local t0=$(date +%s)
  echo "=== $name" | tee -a "$D/summary.txt"
  "$G" -mode openloop -url $URL -queue bq-$name "$@" > "$D/$name.log" 2>&1
  local t1=$(date +%s)
  local db=$(PGPASSWORD=postgres psql -h localhost -U postgres -tAc "select pg_database_size('postgres')" 2>/dev/null)
  echo "$name,$t0,$t1,${db:-0}" >> "$D/marks.csv"
  # full [final] line, including the "(msgs: ...)" counts the template strips:
  # in FAT100 offered/achieved/shed are REQUESTS, the msgs counts are messages.
  grep -E "^\[final\]" "$D/$name.log" | tee -a "$D/summary.txt"
  sleep 3
}

for r in $REGIMES; do case $r in
  A20k)  run A20k  -rate 20000 -push-batch 10 -partitions 100 -consumers 32 -pop-batch 200 -manual-ack -payload 256 -duration 40 -ramp-sec 5;;
  A50k)  run A50k  -rate 50000 -push-batch 10 -partitions 100 -consumers 48 -pop-batch 200 -manual-ack -payload 256 -duration 40 -ramp-sec 5;;
  B1)    run B1    -rate 2000 -push-batch 1 -partitions 16 -consumers 16 -pop-batch 1 -pop-wildcard=false -manual-ack -payload 256 -duration 30;;
  C1000) run C1000 -rate 3000 -push-batch 1 -partitions 1000 -consumers 64 -pop-batch 50 -manual-ack -payload 256 -duration 30 -ramp-sec 3;;
  D1)    run D1    -rate 500 -push-batch 1 -partitions 1 -consumers 1 -pop-batch 1 -pop-wildcard=false -manual-ack -payload 256 -duration 20;;
  # Fat-batch push-only: batches of 100, NO consumers, offered as high as the
  # loader sustains on this VM (calibrate with FAT_RATE, see RESULTS.md).
  FAT100) run FAT100 -rate $FAT_RATE -push-batch 100 -partitions 100 -consumers 0 -payload 256 \
                     -duration $FAT_DURATION -ramp-sec 5 -max-inflight 4096 -idle-conns 4096;;
  *) echo "unknown regime $r" | tee -a "$D/summary.txt";;
esac; done

# -------------------------------------------------------------------- teardown
touch "$D/stop"; sleep $((SAMPLE_SEC + 1)); kill $SPID 2>/dev/null
kill "$(cat "$D/pid")" 2>/dev/null; sleep 3; kill -9 "$(cat "$D/pid")" 2>/dev/null
rm -f "$D/stop"

# ------------------------------------------------------- per-regime CPU/RSS
{ echo "regime,role,window_s,cores,rss_max_mb,rss_avg_mb,pss_max_mb,samples"
  # queen and postgres are long-lived: cores = delta of cumulative CPU between
  # the first and last in-window samples. goload starts and exits inside the
  # window, so its cores = cumulative CPU at the last sample over the time
  # since the window opened.
  awk -F, 'NR>1 && FILENAME ~ /marks/ { s[$1]=$2; e[$1]=$3; next }
           FILENAME ~ /cpu-rss/ && NR>1 {
             ts=$1; role=$2; cpu=$4; rss=$5; pss=$6
             for (r in s) if (ts >= s[r] && ts <= e[r]) {
               k=r SUBSEP role
                 if (!(k in n)) { t0[k]=ts; c0[k]=cpu; w0[k]=s[r]; rmax[k]=0; pmax[k]=0; rsum[k]=0; n[k]=0 }
               t1[k]=ts; c1[k]=cpu
               if (rss > rmax[k]) rmax[k]=rss
               if (pss > pmax[k]) pmax[k]=pss
               rsum[k]+=rss; n[k]++
             }
           }
           END { for (k in n) { split(k, a, SUBSEP)
                   if (a[2] == "goload") { w = t1[k]-w0[k]; cores = (w>0) ? c1[k]/w : 0 }
                   else { w = t1[k]-t0[k]; cores = (w>0) ? (c1[k]-c0[k])/w : 0 }
                   printf "%s,%s,%d,%.2f,%.0f,%.0f,%.0f,%d\n", a[1], a[2], w, cores,
                          rmax[k]/1024, rsum[k]/n[k]/1024, pmax[k]/1024, n[k] } }' \
      "$D/marks.csv" "$D/cpu-rss.csv" | sort
} | tee "$D/cpu-summary.txt" | tee -a "$D/summary.txt"

{ echo "broker log: $(grep -ciE ' error |panic' "$D/broker.log") error lines"
  echo "db size end: $(PGPASSWORD=postgres psql -h localhost -U postgres -tAc "select pg_size_pretty(pg_database_size('postgres'))" 2>/dev/null)"
  echo "disk: $(df -h /root | awk 'NR==2{print $4" free of "$2}')"
  echo "finished: $(date -Is)"
} | tee -a "$D/summary.txt"
