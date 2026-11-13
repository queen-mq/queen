#!/usr/bin/env bash
# perf-n1c.sh (on n1, as root): for a frame-pointer build. Wait until the broker's queen-planner thread has been busy
# (>= 0.5 core) for 5 s in a row, then record frame-pointer stacks of that thread (12 s, 1999 Hz) and of the whole
# broker (6 s, 499 Hz), and write both as `perf script` text (gzipped) for stacks.py. Output to /root/p3/prof/, DONE
# written last.
set -u
OUT=/root/p3/prof
mkdir -p $OUT
rm -f $OUT/*
ticks() { awk '{print $14 + $15}' /proc/$1/task/$2/stat 2>/dev/null || echo 0; }
pid=""; tid=""; prev=0; busy=0
for i in $(seq 1 900); do
  p=$(pgrep -x queen | head -1)
  t=""
  [ -n "$p" ] && t=$(ps -T -p "$p" -o spid=,comm= | awk '$2 == "queen-planner" {print $1; exit}')
  if [ -z "$t" ]; then pid=""; tid=""; busy=0; sleep 1; continue; fi
  if [ "$p" != "$pid" ] || [ "$t" != "$tid" ]; then pid=$p; tid=$t; prev=$(ticks "$pid" "$tid"); busy=0; sleep 1; continue; fi
  now=$(ticks "$pid" "$tid")
  if [ $((now - prev)) -ge 50 ]; then busy=$((busy + 1)); else busy=0; fi
  prev=$now
  [ $busy -ge 5 ] && break
  sleep 1
done
if [ $busy -lt 5 ]; then echo "the planner never got busy" > $OUT/error.txt; touch $OUT/DONE; exit 1; fi
echo "pid $pid planner tid $tid busy at $(date -u +%H:%M:%S)"
sleep 4
echo "planner record start $(date -u +%H:%M:%S)"
perf record -t "$tid" -F 1999 --call-graph fp -o $OUT/planner.data -- sleep 12 2>&1 | tail -1
echo "whole record start $(date -u +%H:%M:%S)"
perf record -p "$pid" -F 499 --call-graph fp -o $OUT/all.data -- sleep 6 2>&1 | tail -1
echo "scripts $(date -u +%H:%M:%S)"
perf script -i $OUT/planner.data -F tid,period,ip,sym,dso --no-inline 2>/dev/null | gzip -1 > $OUT/planner-script.txt.gz
perf script -i $OUT/all.data -F comm,tid,period,ip,sym,dso --no-inline 2>/dev/null | gzip -1 > $OUT/all-script.txt.gz
perf report -i $OUT/planner.data --no-children --sort symbol --stdio -g none --percent-limit 0.3 --no-inline > $OUT/planner-self.txt 2>&1
echo "done $(date -u +%H:%M:%S)"
touch $OUT/DONE
