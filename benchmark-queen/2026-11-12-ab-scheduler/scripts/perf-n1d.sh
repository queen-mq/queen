#!/usr/bin/env bash
# perf-n1d.sh (on n1, as root): for ANY build (no frame pointers needed). Wait until the broker process has been busy
# (>= 8 cores) for 5 s in a row, then, 10 s later, record the whole broker FLAT (no stacks) for 15 s at 499 Hz, and
# write the CPU by thread name and by thread name + symbol. Output to /root/p3/prof/, DONE written last.
set -u
OUT=/root/p3/prof
mkdir -p $OUT
rm -f $OUT/*
ticks() { awk '{print $14 + $15}' /proc/$1/stat 2>/dev/null || echo 0; }
pid=""; prev=0; busy=0
for i in $(seq 1 900); do
  p=$(pgrep -x queen | head -1)
  if [ -z "$p" ]; then pid=""; busy=0; sleep 1; continue; fi
  if [ "$p" != "$pid" ]; then pid=$p; prev=$(ticks "$pid"); busy=0; sleep 1; continue; fi
  now=$(ticks "$pid")
  if [ $((now - prev)) -ge 800 ]; then busy=$((busy + 1)); else busy=0; fi
  prev=$now
  [ $busy -ge 5 ] && break
  sleep 1
done
if [ $busy -lt 5 ]; then echo "the broker never got busy" > $OUT/error.txt; touch $OUT/DONE; exit 1; fi
echo "pid $pid busy at $(date -u +%H:%M:%S)"
sleep 10
t0=$(ticks "$pid")
echo "record start $(date -u +%H:%M:%S)"
perf record -p "$pid" -F 499 -o $OUT/flat.data -- sleep 15 2>&1 | tail -1
t1=$(ticks "$pid")
echo "cores $(( (t1 - t0) / 15 ))e-2 over the record" > $OUT/cores.txt
echo "reports $(date -u +%H:%M:%S)"
perf report -i $OUT/flat.data --no-children --sort comm --stdio > $OUT/flat-comm.txt 2>&1
perf report -i $OUT/flat.data --no-children --sort comm,dso,symbol --stdio --percent-limit 0.03 > $OUT/flat-comm-sym.txt 2>&1
perf report -i $OUT/flat.data --no-children --sort dso,symbol --stdio --percent-limit 0.03 > $OUT/flat-sym.txt 2>&1
echo "done $(date -u +%H:%M:%S)"
touch $OUT/DONE
