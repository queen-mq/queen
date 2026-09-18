#!/bin/bash
# WP-1.11 deliverable 2 — the flatness test of PLAN_RAFT.md §13.6 (G-3, I8) on
# raft1, at a SHRUNK size (see the Deferred note in RESULTS.md). I8: latency,
# CPU, disk and RSS stay flat regardless of how much is already in the store,
# because payloads live in sealed segment files whose index is on disk (.qidx)
# and the dedup/state keyspaces are a disk-backed mmap (heed, MDB_NOSYNC).
#
# It produces four RESULTS files (the format of test/raft/flatness/results.go):
#   empty-A20k / preloaded-A20k, empty-C1000 / preloaded-C1000
# and runs `flatness compare` on each pair. Acceptance: every gated metric
# within ±15% and RSS drift within ±5% inside a run.
#
# The broker runs with QUEEN_RAFT_PIPELINE=1 (WP-1.11 finding F-1: the ratified
# default of 4 stops the node under concurrent load). Both sides of every
# comparison use the same config, so the I8 comparison is valid; the ABSOLUTE
# numbers are pipeline=1 numbers and are not the O14 comparison.
#
# usage: flatness-raft1.sh [minutes] [preload_msgs]
set -u
ulimit -n 262144 2>/dev/null || true   # raft broker holds many segment + log fds (WP-1.11 F-2)

MIN=${1:-4}
PRELOAD=${2:-5000000}
DUR=$((MIN * 60))

BIN=${QUEEN_BIN:-/root/raft/wp111/queen/server/target/release/queen}
G=${GOLOAD:-/root/goload}
FLAT=${FLAT:-/root/raft/wp111/flatness}      # the compiled flatness tool
PORT=${PORT:-6698}
URL=http://127.0.0.1:$PORT
D=${OUTDIR:-/root/raft/wp111/raft1-flatness}
HOST=vm-164.90.215.224
COMMIT=$(md5sum "$BIN" | cut -c1-12)
HZ=$(getconf CLK_TCK); PAGE=$(getconf PAGESIZE)

rm -rf "$D"; mkdir -p "$D"
DATADEV=$(df --output=source "$D" | tail -1 | sed 's#/dev/##; s#[0-9]*$##')
disk_wr(){ awk -v d="$DATADEV" '$3==d{print $10; f=1} END{if(!f)print 0}' /proc/diskstats; }

QPID=""
start_broker(){ # $1 data dir
  rm -rf "$1"; mkdir -p "$1"
  env QUEEN_STORAGE=raft QUEEN_RAFT_DIR="$1" QUEEN_BIND_ADDR=127.0.0.1 PORT=$PORT \
      JWT_ENABLED=false QUEEN_TENANCY_HEADER=false QUEEN_RAFT_PIPELINE=1 \
      FILE_BUFFER_DIR="$D/buf" LOG_LEVEL=warn nohup "$BIN" >>"$D/broker.log" 2>&1 &
  QPID=$!
  for i in $(seq 1 90); do curl -s $URL/health 2>/dev/null | grep -q '"storageReady":true' && return 0; kill -0 $QPID 2>/dev/null || return 1; sleep 0.3; done
  return 1
}
stop_broker(){ [ -n "$QPID" ] && { kill $QPID 2>/dev/null; sleep 2; kill -9 $QPID 2>/dev/null; }; QPID=""; }

# sample queen RSS(kb)+cpu(s)+disk_wr(sectors) every 5s to $1 while $2 (a marker file) is absent
sample_to(){ local csv=$1 stop=$2; echo "ts,cpu_s,rss_kb,wr" > "$csv"
  while [ ! -f "$stop" ]; do
    if [ -r /proc/$QPID/stat ]; then
      read u s r <<< "$(awk '{sub(/^[0-9]+ \(.*\) /,""); print $12,$13,$22}' /proc/$QPID/stat 2>/dev/null)"
      awk -v ts=$(date +%s) -v c=$(( ${u:-0}+${s:-0} )) -v hz=$HZ -v rss=${r:-0} -v pg=$PAGE -v wr=$(disk_wr) \
        'BEGIN{printf "%d,%.2f,%d,%d\n", ts, c/hz, rss*pg/1024, wr}' >> "$csv"
    fi
    sleep 5
  done
}

# one regime run -> a RESULTS file. $1 regime, $2 state label, $3 out file
run_regime(){ local regime=$1 state=$2 out=$3
  local log=$D/$state-$regime.gl csv=$D/$state-$regime.csv stop=$D/.stop
  rm -f "$stop"
  sample_to "$csv" "$stop" & local spid=$!
  case $regime in
    A20k)  "$G" -mode openloop -url $URL -queue fl-$regime -rate 20000 -push-batch 10 -partitions 100 -consumers 32 -pop-batch 200 -manual-ack -payload 256 -duration $DUR -ramp-sec 5 > "$log" 2>&1;;
    C1000) "$G" -mode openloop -url $URL -queue fl-$regime -rate 3000 -push-batch 1 -partitions 1000 -consumers 64 -pop-batch 50 -manual-ack -payload 256 -duration $DUR -ramp-sec 3 > "$log" 2>&1;;
  esac
  touch "$stop"; sleep 6; kill $spid 2>/dev/null
  # metrics
  local fin=$(grep -E '^\[final\]' "$log")
  local p50=$(echo "$fin"  | sed -n 's/.*overall p50=\([0-9.]*\).*/\1/p')
  local p99=$(echo "$fin"  | sed -n 's/.* p99=\([0-9.]*\).*/\1/p')
  local p999=$(echo "$fin" | sed -n 's/.* p999=\([0-9.]*\).*/\1/p')
  local ack=$(echo "$fin"  | sed -n 's/.*ackAvg=\([0-9.]*\)ms.*/\1/p')
  # cpu cores = delta cpu / window; rss start/end; disk mbps (sectors*512)
  read cores rss0 rss1 mbps <<< "$(awk -F, 'NR==2{c0=$2; t0=$1; w0=$4; r0=$3} NR>1{c1=$2;t1=$1;w1=$4;r1=$3; if(NR==2)rss0=$3} END{
      win=t1-t0; cores=(win>0)?(c1-c0)/win:0; mbps=(win>0)?(w1-w0)*512/1048576/win:0;
      printf "%.2f %.0f %.0f %.1f", cores, r0/1024, r1/1024, mbps}' "$csv")"
  { echo "# regime=$regime state=$state host=$HOST commit=$COMMIT"
    echo "# duration_s=$DUR topology=raft1 pipeline=1 command=\"goload $regime ${MIN}min\""
    printf "%-18s %s\n" p50_ms "${p50:-0}"
    printf "%-18s %s\n" p99_ms "${p99:-0}"
    printf "%-18s %s\n" p999_ms "${p999:-0}"
    printf "%-18s %s\n" ack_rtt_ms "${ack:-0}"
    printf "%-18s %s\n" cpu_cores "${cores:-0}"
    printf "%-18s %s\n" disk_mbps "${mbps:-0}"
    printf "%-18s %s\n" rss_start_mb "${rss0:-0}"
    printf "%-18s %s\n" rss_end_mb "${rss1:-0}"
  } > "$out"
  echo "  $state $regime: p50=$p50 p99=$p99 p999=$p999 ack=$ack cores=$cores rss ${rss0}->${rss1}MB disk=${mbps}MB/s"
}

preload(){ # push PRELOAD msgs, no consumers, into a SEPARATE at-rest queue
  # (queue "flbg", 1000 partitions) that the A20k / C1000 regimes never pop — so
  # the backlog sits at rest and the regime measures fresh-traffic latency ON TOP
  # of a large store (the I8 question), instead of the regime consuming its own
  # preload.
  echo "  preloading $PRELOAD msgs into at-rest queue flbg (push-only, fat batch, 1000 partitions)"
  local t0=$(date +%s)
  "$G" -mode openloop -url $URL -queue flbg -rate 400000 -push-batch 100 -partitions 1000 -consumers 0 \
       -payload 256 -duration 0 -max-inflight 4096 -idle-conns 4096 > "$D/preload.gl" 2>&1 &
  local gp=$!
  # stop when goload has pushed ~PRELOAD msgs. NOTE: a goload report line has
  # TWO "push=" (the running count AND "errs push="), so take the count field of
  # the LAST report line via awk, not `grep push= | tail -1` (which grabs the
  # errs field and never advances).
  while kill -0 $gp 2>/dev/null; do
    local pushed=$(awk -F'push=' '/^\[[0-9]/{n=$2+0} END{print n+0}' "$D/preload.gl")
    [ "${pushed:-0}" -ge "$PRELOAD" ] && break
    sleep 2
  done
  kill $gp 2>/dev/null; sleep 1; kill -9 $gp 2>/dev/null
  echo "  preloaded $(awk -F'push=' '/^\[[0-9]/{n=$2+0} END{print n+0}' "$D/preload.gl") msgs in $(( $(date +%s)-t0 ))s, data dir $(du -sh "$D/data-P" 2>/dev/null | cut -f1)"
}

echo "flatness-raft1: ${MIN}min runs, preload $PRELOAD, commit $COMMIT"

# ---- empty baselines (fresh store per regime) ----
start_broker "$D/data-EA" || { echo "broker E-A20k failed"; exit 1; }
run_regime A20k empty "$D/empty-A20k.results"
stop_broker
start_broker "$D/data-EC" || { echo "broker E-C1000 failed"; exit 1; }
run_regime C1000 empty "$D/empty-C1000.results"
stop_broker

# ---- preloaded (one big store, both regimes on top) ----
start_broker "$D/data-P" || { echo "broker P failed"; exit 1; }
preload
run_regime A20k preloaded "$D/preloaded-A20k.results"
run_regime C1000 preloaded "$D/preloaded-C1000.results"
stop_broker

echo "== flatness compare A20k =="
"$FLAT" compare -baseline "$D/empty-A20k.results" -candidate "$D/preloaded-A20k.results" | tee "$D/compare-A20k.txt"
echo "== flatness compare C1000 =="
"$FLAT" compare -baseline "$D/empty-C1000.results" -candidate "$D/preloaded-C1000.results" | tee "$D/compare-C1000.txt"
echo "broker error lines: $(grep -ciE ' error |panic|poison' "$D/broker.log")"
