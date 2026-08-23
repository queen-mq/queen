#!/bin/bash
# Full-quota soak driver. Runs on the LOADER (queen-02), drives the cell through
# the proxy over the VPC.
#
#   ./soak-run.sh <label> <duration_s> <free> <dev> <pro>
#
# Every tenant sits at its plan ceiling, so the answer is a GUARANTEED floor:
# how many tenants a cell serves when all of them max out, rather than a number
# resting on a contention assumption.
#
#   plan  queues x partitions  total parts  msg/s  per queue
#   free      2 x   500              1 000      5      2.5
#   dev       5 x 2 000             10 000     25      5
#   pro      20 x 5 000            100 000    100      5
#
# Partitions-per-queue is unlimited within the plan total, so wide queues are a
# legitimate tenant shape; goload drives one queue per process, which is why the
# model uses few wide queues rather than the plan's maximum queue count.
set -uo pipefail
# SKIP_CONFIGURE=1 assumes the queues and partitions already exist. Creating
# 1.175M partitions measured ~16 minutes at ~71k/min, so a short run that also
# configures spends its whole life provisioning and never reaches steady state.
# Create once, then measure.
#
# POP CALL RATE is reduced by using FEWER CONSUMERS per queue, not by holding
# pops. minPopWait would trade delivery latency directly against call volume and
# there is no room for that against a 200 ms p99 SLO. Fewer consumers means each
# one accumulates more per call at the same latency.
#
# POP SHAPE. pop-partitions was set to the FULL per-queue partition count, so a
# single consumer asked the broker to sweep 5,000 partitions to collect the ~5
# msg/s that queue carries: cands_visit=1.2, pop = 47% of all Postgres time at
# 6.93 ms/call. The crossbench rule is pop-partitions ~ lanes/25, and 5,000 was
# 25x too wide. Now: 3 consumers per queue, 10 partitions per pop, batch 100.
#
# CONNECTION SIZING is load-bearing on a small loader. goload auto-sizes idle
# connections per TENANT CLIENT from max-inflight, which gave 414 each: with 27
# processes x ~325 tenant clients that is ~134k sockets against a ~55k ephemeral
# port range. The result was "cannot assign requested address" and load average
# 229 on 4 cores, which looks exactly like a broken cell and is not.

PROXY=http://10.114.0.2:6711
CELL=10.114.0.2
LABEL=${1:?label}; DUR=${2:?duration_s}; NFREE=${3:-25}; NDEV=${4:-15}; NPRO=${5:-10}
OUT=/root/soak/$LABEL; mkdir -p "$OUT"
DRAIN=${DRAIN:-60}

TOTRATE=$(( NFREE*5 + NDEV*25 + NPRO*100 ))
TOTPARTS=$(( NFREE*1000 + NDEV*10000 + NPRO*100000 ))
echo "################ $LABEL — $((NFREE+NDEV+NPRO)) tenants at FULL QUOTA"
echo "    free=$NFREE dev=$NDEV pro=$NPRO | ${TOTRATE} msg/s | ${TOTPARTS} partitions | ${DUR}s"
echo "    started $(date -u +%FT%TZ)"

# samplers: cell containers, and the loader itself (SPEC 5.1 voids any window
# where the load generator was the bottleneck)
ssh -o BatchMode=yes root@$CELL "rm -f /root/samples/$LABEL.csv
  setsid nohup /root/sampler.sh cell-pg cell-broker-a cell-broker-b cell-proxy cell-lb \
    > /root/samples/$LABEL.csv 2>/dev/null </dev/null & echo \$! > /root/cellsampler.pid" >/dev/null
rm -f "$OUT/loader.csv"
setsid nohup /root/sampler.sh > "$OUT/loader.csv" 2>/dev/null </dev/null &
echo $! > /root/loadsampler.pid

# The storage trend collector is a SHIPPED FILE (/root/dbtrend.sh), started
# separately. It used to be generated here by a nested heredoc, whose escaping
# turned $(...) into a literal and silently produced a header and no rows — and
# it overwrote the fixed copy on every launch, so it broke again each restart.
ssh -o BatchMode=yes root@$CELL "[ -f /root/dbtrend.pid ] && kill \$(cat /root/dbtrend.pid) 2>/dev/null
  rm -f /root/samples/$LABEL-db.csv
  setsid nohup /root/dbtrend.sh > /root/samples/$LABEL-db.csv 2>/dev/null </dev/null &
  echo \$! > /root/dbtrend.pid" >/dev/null

# Retention must match the PLAN, not a convenience value: with a 5-minute
# completed-retention the storage curve plateaus in minutes and the soak learns
# nothing. free 1d / dev 3d / pro 7d is what the plans actually sell.
plan_retention() {
  case "$1" in
    free) echo 86400 ;;
    dev)  echo 259200 ;;
    pro)  echo 604800 ;;
    *)    echo 86400 ;;
  esac
}

launch() { # <plan> <file> <n> <queues> <parts> <rate_per_queue>
  local plan=$1 file=$2 n=$3 q=$4 p=$5 r=$6
  local ret; ret=$(plan_retention "$plan")
  [ "$n" -eq 0 ] && return 0
  for i in $(seq 1 "$q"); do
    /root/goload -mode cloud -target proxy -url "$PROXY" \
      -tenants-file "$file" -tenants "$n" -queue "${plan}-q$i" \
      -per-tenant-rate "$r" -push-batch 1 \
      -partitions "$p" -pop-partitions "${POP_PARTS:-10}" -pop-batch "${POP_BATCH:-100}" \
      -consumers-per-tenant "${CONSUMERS:-3}" -pop-wait -pop-timeout 5000 ${SKIP_CONFIGURE:+-skip-configure} \
      -idle-conns ${IDLE_CONNS:-8} -max-inflight ${MAX_INFLIGHT:-256} \
      -payload 256 -duration "$DUR" -drain "$DRAIN" -report 300 \
      -completed-retention "$ret" -pending-retention "$ret" \
      -out "$OUT" -run-id "$plan-q$i" > "$OUT/$plan-q$i.log" 2>&1 &
    PIDS+=($!)
  done
}

PIDS=()
launch free /root/soak/t-free.json "$NFREE" 2  500  2.5
launch dev  /root/soak/t-dev.json  "$NDEV"  5  2000 5
launch pro  /root/soak/t-pro.json  "$NPRO"  20 5000 5
echo "    launched ${#PIDS[@]} loader processes"
for p in "${PIDS[@]}"; do wait "$p" || true; done

[ -f /root/loadsampler.pid ] && kill "$(cat /root/loadsampler.pid)" 2>/dev/null
ssh -o BatchMode=yes root@$CELL '[ -f /root/cellsampler.pid ] && kill $(cat /root/cellsampler.pid) 2>/dev/null
  [ -f /root/dbtrend.pid ] && kill $(cat /root/dbtrend.pid) 2>/dev/null; true' >/dev/null
scp -q root@$CELL:/root/samples/$LABEL.csv    "$OUT/cell.csv"    2>/dev/null
scp -q root@$CELL:/root/samples/$LABEL-db.csv "$OUT/db.csv"      2>/dev/null

# verdict: worst p99 across EVERY queue of every plan is the tenant's experience
WORST=0; MISS=0; DUP=0; R429=0; CONF=0
for f in "$OUT"/*-q*.log; do
  [ -f "$f" ] || continue
  p99=$(grep -oE "p99= *[0-9.]+" "$f" | tail -1 | sed 's/.*p99= *//')
  [ -n "$p99" ] && WORST=$(awk -v a="$WORST" -v b="$p99" 'BEGIN{print (b>a)?b:a}')
  m=$(grep -E "^     TOTAL" "$f" | tail -1 | awk '{print $4+0}'); MISS=$((MISS + ${m:-0}))
  d=$(grep -E "^     TOTAL" "$f" | tail -1 | awk '{print $5+0}'); DUP=$((DUP + ${d:-0}))
  r=$(grep -oE "http_429:[0-9]+" "$f" | tail -1 | sed 's/.*://'); R429=$((R429 + ${r:-0}))
  grep -q "FAILED — a run over half" "$f" && CONF=$((CONF+1))
done
V=$(awk -v w="$WORST" 'BEGIN{print (w>0 && w<=200)?"WITHIN SLO":"OVER SLO"}')
echo "--- $LABEL: $((NFREE+NDEV+NPRO)) tenants, ${TOTRATE} msg/s -> worst p99 ${WORST} ms  $V"
echo "    missing=$MISS dup=$DUP 429=$R429 configFail=$CONF   ended $(date -u +%FT%TZ)"
