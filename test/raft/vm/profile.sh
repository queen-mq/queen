#!/bin/bash
# PERF-1 — CPU profile of the raft1 broker under one goload regime, on the Linux
# VM (PLAN_RAFT.md O18, §13.6). Boots ONE broker on QUEEN_STORAGE=raft at
# QUEEN_RAFT_PIPELINE=4, drives it with a goload regime (the same flags as
# measure-raft1.sh), records `perf record -g` for a window, and writes a
# folded-stack summary plus top-40 self/inclusive symbol tables into a results
# dir. Pairs with timing.sh, which scrapes /metrics/prometheus alongside.
#
# usage: profile.sh [regime] [perf_seconds] ["<extra broker env>"]
#   regime:       A20k A50k B1 C1000 D1 FAT100  (default A20k)
#   perf_seconds: the perf record window            (default 60)
#   extra env:    e.g. "QUEEN_RAFT_METRICS=0" to profile the ablated build
#
# env overrides (same defaults as measure-raft1.sh):
#   QUEEN_BIN   broker binary  (default /root/raft/wp111/queen/server/target/release/queen)
#   GOLOAD      loader binary  (default /root/goload)
#   OUTDIR      results dir     (default /root/raft/perf/profile-<regime>-<ts>)
#   BROKER_PORT broker HTTP port(default 6699)
#   PERF_FREQ   perf sample Hz  (default 99)
#
# NOTHING is left running and the data dir is deleted at the end (§0.3).
set -u

REGIME=${1:-A20k}
PERF_SECONDS=${2:-60}
EXTRA=${3:-}

BIN=${QUEEN_BIN:-/root/raft/wp111/queen/server/target/release/queen}
G=${GOLOAD:-/root/goload}
BROKER_PORT=${BROKER_PORT:-6699}
PERF_FREQ=${PERF_FREQ:-99}
TS=$(date +%Y%m%d-%H%M%S)
D=${OUTDIR:-/root/raft/perf/profile-$REGIME-$TS}
DATADIR=$D/data
URL=http://127.0.0.1:$BROKER_PORT

ulimit -n 262144 2>/dev/null || true

[ -x "$BIN" ] || { echo "no broker binary at $BIN"; exit 1; }
[ -x "$G" ]   || { echo "no goload at $G"; exit 1; }

# perf must exist; the task allows installing it (nothing else system-wide).
if ! command -v perf >/dev/null 2>&1; then
  echo "perf not found; installing linux-tools (allowed by PERF-1)"
  apt-get install -y linux-tools-common "linux-tools-$(uname -r)" linux-tools-generic \
    >/dev/null 2>&1 || { echo "could NOT install perf; aborting"; exit 1; }
fi

rm -rf "$D"; mkdir -p "$DATADIR"

# the goload flags per regime (copied verbatim from measure-raft1.sh).
case $REGIME in
  A20k)  GARGS="-rate 20000 -push-batch 10 -partitions 100 -consumers 32 -pop-batch 200 -manual-ack -payload 256 -ramp-sec 5";;
  A50k)  GARGS="-rate 50000 -push-batch 10 -partitions 100 -consumers 48 -pop-batch 200 -manual-ack -payload 256 -ramp-sec 5";;
  B1)    GARGS="-rate 2000 -push-batch 1 -partitions 16 -consumers 16 -pop-batch 1 -pop-wildcard=false -manual-ack -payload 256";;
  C1000) GARGS="-rate 3000 -push-batch 1 -partitions 1000 -consumers 64 -pop-batch 50 -manual-ack -payload 256 -ramp-sec 3";;
  D1)    GARGS="-rate 500 -push-batch 1 -partitions 1 -consumers 1 -pop-batch 1 -pop-wildcard=false -manual-ack -payload 256";;
  FAT100) GARGS="-rate 300000 -push-batch 100 -partitions 100 -consumers 0 -payload 256 -ramp-sec 5 -max-inflight 4096 -idle-conns 4096";;
  *) echo "unknown regime $REGIME"; exit 1;;
esac
# goload runs long enough to cover the whole perf window + the ramp.
GDUR=$((PERF_SECONDS + 15))

{ echo "host: $(hostname) $(uname -sr) $(nproc) vCPU"
  echo "binary: $BIN  md5 $(md5sum "$BIN" | cut -c1-12)"
  echo "loader: $G  md5 $(md5sum "$G" | cut -c1-12)"
  echo "regime: $REGIME   perf window: ${PERF_SECONDS}s @ ${PERF_FREQ}Hz"
  echo "extra broker env: ${EXTRA:-<none>}"
  echo "started: $(date -Is)"
} | tee "$D/summary.txt"

# --------------------------------------------------------------------- broker
(pkill -x queen 2>/dev/null; true); sleep 1
env QUEEN_STORAGE=raft QUEEN_RAFT_DIR="$DATADIR" QUEEN_RAFT_PIPELINE=4 \
    QUEEN_BIND_ADDR=127.0.0.1 PORT=$BROKER_PORT JWT_ENABLED=false \
    QUEEN_TENANCY_HEADER=false FILE_BUFFER_DIR="$D/buffers" LOG_LEVEL=warn \
    $EXTRA nohup "$BIN" > "$D/broker.log" 2>&1 &
QPID=$!
echo $QPID > "$D/pid"
ready=""
for i in $(seq 1 90); do
  curl -s $URL/health 2>/dev/null | grep -q '"storageReady":true' && { ready=1; break; }
  kill -0 $QPID 2>/dev/null || { echo "broker exited early, see broker.log" | tee -a "$D/summary.txt"; exit 1; }
  sleep 1
done
[ -n "$ready" ] || { echo "broker not storageReady, see broker.log" | tee -a "$D/summary.txt"; kill $QPID 2>/dev/null; exit 1; }
echo "broker pid $QPID storageReady on $BROKER_PORT" | tee -a "$D/summary.txt"

# ------------------------------------------------------------------- the load
echo "goload $REGIME for ${GDUR}s: $GARGS" | tee -a "$D/summary.txt"
"$G" -mode openloop -url $URL -queue bq-prof-$REGIME $GARGS -duration $GDUR \
    > "$D/goload.log" 2>&1 &
GLPID=$!
sleep 5   # let the ramp reach the target rate before the window opens

# ------------------------------------------------------------------- perf
echo "perf record -g -F $PERF_FREQ -p $QPID -- sleep $PERF_SECONDS" | tee -a "$D/summary.txt"
perf record -g -F "$PERF_FREQ" -p "$QPID" -o "$D/perf.data" -- sleep "$PERF_SECONDS" \
    2> "$D/perf-record.log" || echo "perf record returned non-zero (see perf-record.log)" | tee -a "$D/summary.txt"

# stop the load and the broker before post-processing.
kill $GLPID 2>/dev/null
wait $GLPID 2>/dev/null
grep -E "^\[final\]" "$D/goload.log" | tee -a "$D/summary.txt"

# ------------------------------------------------------------- folded + tables
# Folded stacks (Brendan Gregg shape): every sampled stack on one line,
# root;...;leaf <count>. Built with stackcollapse-perf.pl if it is on the box,
# else with a self-contained awk fold of `perf script` (no extra tooling).
perf script -i "$D/perf.data" > "$D/perf.script" 2> "$D/perf-script.log" || true
if command -v stackcollapse-perf.pl >/dev/null 2>&1; then
  stackcollapse-perf.pl "$D/perf.script" > "$D/folded.txt" 2>/dev/null
else
  awk '
    function flush() { if (stk != "") print stk" "1; stk="" }
    /^$/ { flush(); next }
    /^[0-9a-f]+ / || /^\t/ {
      # a stack frame line: "  ADDR symbol (dso)" — take the symbol column.
      sym=$2; if (sym=="") next
      stk = (stk=="") ? sym : sym";"stk
      next
    }
    { flush() }   # a sample header line ends the previous stack
    END { flush() }
  ' "$D/perf.script" | awk '{ c[$0]++ } END { for (k in c){ sub(/ 1$/,"",k); print k" "c[k] } }' \
    > "$D/folded.txt"
fi

# Top-40 self time (leaf of each folded stack, weighted by sample count).
awk '{ n=split($1,f,";"); leaf=f[n]; self[leaf]+=$2; tot+=$2 }
     END { for (s in self) printf "%d\t%.2f%%\t%s\n", self[s], 100*self[s]/tot, s }' \
    "$D/folded.txt" | sort -rn | head -40 > "$D/top40-self.txt"

# Top-40 inclusive time (any frame containing the symbol, weighted).
awk '{ n=split($1,f,";"); delete seen
       for (i=1;i<=n;i++){ if(!(f[i] in seen)){ inc[f[i]]+=$2; seen[f[i]]=1 } }
       tot+=$2 }
     END { for (s in inc) printf "%d\t%.2f%%\t%s\n", inc[s], 100*inc[s]/tot, s }' \
    "$D/folded.txt" | sort -rn | head -40 > "$D/top40-inclusive.txt"

# perf's own view, as a cross-check that needs no folding.
perf report -i "$D/perf.data" --stdio -n --sort symbol -g none 2>/dev/null \
    | grep -vE '^#|^$' | head -40 > "$D/perf-report-self.txt" || true

{ echo "---- top 40 by SELF ----"; cat "$D/top40-self.txt"
  echo; echo "---- top 40 by INCLUSIVE ----"; cat "$D/top40-inclusive.txt"
  echo; echo "broker errors: $(grep -ciE ' error |panic' "$D/broker.log")"
  echo "results in: $D"; echo "finished: $(date -Is)"
} | tee -a "$D/summary.txt"

# -------------------------------------------------------------------- teardown
kill "$(cat "$D/pid")" 2>/dev/null; sleep 2; kill -9 "$(cat "$D/pid")" 2>/dev/null
rm -rf "$DATADIR" "$D/buffers" "$D/perf.script"
echo "cleaned: broker stopped, data dir removed (kept perf.data, folded.txt, tables)"
