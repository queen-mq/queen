#!/bin/bash
# WP-1.11 — raft1 message-path baseline on the Linux VM (PLAN_RAFT.md §13.6,
# Appendix G, O14). The raft1 sibling of measure-baseline.sh: ONE broker on
# QUEEN_STORAGE=raft (LocalReplicator, phase 1), NO Postgres, the SAME goload
# regimes and the SAME CPU/RSS sampler, so its columns line up under WP-0.2's
# postgres numbers (M1) for the O14 comparison.
#
# What differs from measure-baseline.sh, and only that:
#   * the broker boots on the replicated state machine (QUEEN_STORAGE=raft,
#     QUEEN_RAFT_DIR), no PG_* env, no schema apply, no DB drop;
#   * readiness gates on /health .raft.storageReady == true (§11.5 step 6),
#     not on a plain 200;
#   * the sampler tracks queen + goload only (there is no postgres);
#   * a disk-throughput column (write MB/s on the data device) replaces the PG
#     PSS column, because the store IS the disk here (I8);
#   * the raft-mode defaults already ARE §13.6's flatness settings — an
#     implicitly-created queue gets dedup_window_seconds=3600 and retention
#     off (server/src/rsm/facade/real.rs default_queue_config), and the raft
#     facade serves no /api/v1/configure, so goload's -dedup-window is a no-op
#     here and nothing configures retention: it is off by default.
#
# usage: measure-raft1.sh <tag> ["<regimes>"] ["<extra broker env>"]
#   regimes: any of A20k A50k B1 C1000 D1 FAT100 (default: all six).
# env overrides:
#   QUEEN_BIN   broker binary   (default /root/raft/wp111/queen/server/target/release/queen)
#   GOLOAD      loader binary   (default /root/goload)
#   OUTDIR      output dir      (default /root/raft/wp111/raft1-<tag>)
#   DATADIR     raft data dir   (default <OUTDIR>/data)
#   BROKER_PORT broker HTTP port(default 6698)
#   FAT_RATE / FAT_DURATION / SAMPLE_SEC as in measure-baseline.sh
#   KEEP_DATA=1 to keep the data dir after the run (default: kept; the flatness
#              and noisy-neighbour drivers reuse it).
set -u
ulimit -n 262144 2>/dev/null || true   # raft broker holds many segment + log fds (WP-1.11 F-2)

TAG=${1:-raft1}
REGIMES=${2:-"A20k A50k B1 C1000 D1 FAT100"}
EXTRA=${3:-}

BIN=${QUEEN_BIN:-/root/raft/wp111/queen/server/target/release/queen}
G=${GOLOAD:-/root/goload}
BROKER_PORT=${BROKER_PORT:-6698}
FAT_RATE=${FAT_RATE:-300000}
FAT_DURATION=${FAT_DURATION:-60}
SAMPLE_SEC=${SAMPLE_SEC:-5}
D=${OUTDIR:-/root/raft/wp111/raft1-$TAG}
DATADIR=${DATADIR:-$D/data}
URL=http://127.0.0.1:$BROKER_PORT

[ -x "$BIN" ] || { echo "no broker binary at $BIN"; exit 1; }
[ -x "$G" ]   || { echo "no goload at $G"; exit 1; }

rm -rf "$D"; mkdir -p "$D" "$DATADIR"
HZ=$(getconf CLK_TCK); PAGE=$(getconf PAGESIZE)

# the block device the data dir lives on, for the disk-throughput column
DATADEV=$(df --output=source "$DATADIR" | tail -1 | sed 's#/dev/##; s#[0-9]*$##')

# ---------------------------------------------------------------- fresh broker
(pkill -x queen 2>/dev/null; true); sleep 1

{ echo "host: $(hostname) $(uname -sr) $(nproc) vCPU $(free -g | awk '/^Mem:/{print $2}') GB"
  echo "storage: raft (LocalReplicator, phase 1), data dir $DATADIR on /dev/$DATADEV"
  echo "binary: $BIN"
  echo "binary md5: $(md5sum $BIN | cut -c1-12)"
  echo "loader: $G ($(md5sum $G | cut -c1-12))"
  echo "regimes: $REGIMES"
  echo "started: $(date -Is)"
} | tee -a "$D/summary.txt"

# ------------------------------------------------------------------- broker
env QUEEN_STORAGE=raft QUEEN_RAFT_DIR="$DATADIR" QUEEN_BIND_ADDR=127.0.0.1 \
  PORT=$BROKER_PORT JWT_ENABLED=false QUEEN_TENANCY_HEADER=false \
  FILE_BUFFER_DIR=$D/buffers LOG_LEVEL=warn $EXTRA nohup "$BIN" > "$D/broker.log" 2>&1 &
QPID=$!
echo $QPID > "$D/pid"
ready=""
for i in $(seq 1 90); do
  body=$(curl -s $URL/health 2>/dev/null)
  echo "$body" | grep -q '"storageReady":true' && { ready=1; break; }
  kill -0 $QPID 2>/dev/null || { echo "broker exited early, see $D/broker.log" | tee -a "$D/summary.txt"; exit 1; }
  sleep 1
done
[ -n "$ready" ] || { echo "broker not storageReady, see $D/broker.log" | tee -a "$D/summary.txt"; kill $QPID 2>/dev/null; exit 1; }
echo "broker pid $QPID storageReady on $BROKER_PORT: $(curl -s $URL/health | tr ',' '\n' | grep -iE 'version|storageReady' | head -3 | tr '\n' ' ')" | tee -a "$D/summary.txt"

# ------------------------------------------------------------- CPU/RSS sampler
echo "ts,role,nproc,cpu_s,rss_kb,pss_kb,disk_wr_sectors" > "$D/cpu-rss.csv"
disk_wr_sectors() { awk -v d="$DATADEV" '$3==d{print $10; found=1} END{if(!found)print 0}' /proc/diskstats 2>/dev/null; }
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
  [ "$n" -gt 0 ] || return 0
  local wr=0; [ "$role" = queen ] && wr=$(disk_wr_sectors)
  awk -v ts=$(date +%s) -v role=$role -v n=$n -v cpu=$cpu -v hz=$HZ -v rss=$rss -v pg=$PAGE -v pss=$pss -v wr=$wr \
    'BEGIN{printf "%d,%s,%d,%.2f,%d,%d,%d\n", ts, role, n, cpu/hz, rss*pg/1024, pss, wr}' >> "$D/cpu-rss.csv"
}
sampler() {
  while [ ! -f "$D/stop" ]; do
    sample_role queen "$QPID"
    sample_role goload "$(pgrep -x "$(basename "$G")" | tr '\n' ' ')"
    sleep "$SAMPLE_SEC"
  done
}
sampler & SPID=$!

# ----------------------------------------------------------------- the lanes
echo "regime,start_ts,end_ts,data_bytes_end" > "$D/marks.csv"
run() {
  local name=$1; shift
  local t0=$(date +%s)
  echo "=== $name" | tee -a "$D/summary.txt"
  "$G" -mode openloop -url $URL -queue bq-$name "$@" > "$D/$name.log" 2>&1
  local t1=$(date +%s)
  local db=$(du -sb "$DATADIR" 2>/dev/null | cut -f1)
  echo "$name,$t0,$t1,${db:-0}" >> "$D/marks.csv"
  grep -E "^\[final\]" "$D/$name.log" | tee -a "$D/summary.txt"
  sleep 3
}

for r in $REGIMES; do case $r in
  A20k)  run A20k  -rate 20000 -push-batch 10 -partitions 100 -consumers 32 -pop-batch 200 -manual-ack -payload 256 -duration 40 -ramp-sec 5;;
  A50k)  run A50k  -rate 50000 -push-batch 10 -partitions 100 -consumers 48 -pop-batch 200 -manual-ack -payload 256 -duration 40 -ramp-sec 5;;
  B1)    run B1    -rate 2000 -push-batch 1 -partitions 16 -consumers 16 -pop-batch 1 -pop-wildcard=false -manual-ack -payload 256 -duration 30;;
  C1000) run C1000 -rate 3000 -push-batch 1 -partitions 1000 -consumers 64 -pop-batch 50 -manual-ack -payload 256 -duration 30 -ramp-sec 3;;
  D1)    run D1    -rate 500 -push-batch 1 -partitions 1 -consumers 1 -pop-batch 1 -pop-wildcard=false -manual-ack -payload 256 -duration 20;;
  FAT100) run FAT100 -rate $FAT_RATE -push-batch 100 -partitions 100 -consumers 0 -payload 256 \
                     -duration $FAT_DURATION -ramp-sec 5 -max-inflight 4096 -idle-conns 4096;;
  *) echo "unknown regime $r" | tee -a "$D/summary.txt";;
esac; done

# -------------------------------------------------------------------- teardown
touch "$D/stop"; sleep $((SAMPLE_SEC + 1)); kill $SPID 2>/dev/null
kill "$(cat "$D/pid")" 2>/dev/null; sleep 3; kill -9 "$(cat "$D/pid")" 2>/dev/null
rm -f "$D/stop"

# ------------------------------------------------------- per-regime CPU/RSS/disk
{ echo "regime,role,window_s,cores,rss_max_mb,rss_avg_mb,disk_wr_mbps,samples"
  awk -F, 'NR>1 && FILENAME ~ /marks/ { s[$1]=$2; e[$1]=$3; next }
           FILENAME ~ /cpu-rss/ && NR>1 {
             ts=$1; role=$2; cpu=$4; rss=$5; wr=$7
             for (r in s) if (ts >= s[r] && ts <= e[r]) {
               k=r SUBSEP role
                 if (!(k in n)) { t0[k]=ts; c0[k]=cpu; w0[k]=s[r]; rmax[k]=0; rsum[k]=0; wr0[k]=wr; n[k]=0 }
               t1[k]=ts; c1[k]=cpu; wr1[k]=wr
               if (rss > rmax[k]) rmax[k]=rss
               rsum[k]+=rss; n[k]++
             }
           }
           END { for (k in n) { split(k, a, SUBSEP)
                   if (a[2] == "goload") { w = t1[k]-w0[k]; cores = (w>0) ? c1[k]/w : 0 }
                   else { w = t1[k]-t0[k]; cores = (w>0) ? (c1[k]-c0[k])/w : 0 }
                   # sectors are 512 B; MB/s over the window (queen role only carries wr)
                   ww = t1[k]-t0[k]; mbps = (ww>0 && wr1[k]>0) ? (wr1[k]-wr0[k])*512/1048576/ww : 0
                   printf "%s,%s,%d,%.2f,%.0f,%.0f,%.1f,%d\n", a[1], a[2], w, cores,
                          rmax[k]/1024, rsum[k]/n[k]/1024, mbps, n[k] } }' \
      "$D/marks.csv" "$D/cpu-rss.csv" | sort
} | tee "$D/cpu-summary.txt" | tee -a "$D/summary.txt"

{ echo "broker log: $(grep -ciE ' error |panic' "$D/broker.log") error lines"
  echo "data dir size end: $(du -sh "$DATADIR" 2>/dev/null | cut -f1)"
  echo "disk: $(df -h /root | awk 'NR==2{print $4" free of "$2}')"
  echo "finished: $(date -Is)"
} | tee -a "$D/summary.txt"

[ "${KEEP_DATA:-1}" = "1" ] || rm -rf "$DATADIR"
