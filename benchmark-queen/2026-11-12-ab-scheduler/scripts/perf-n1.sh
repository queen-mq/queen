#!/usr/bin/env bash
# perf-n1.sh (on n1, as root): wait until the broker's queen-planner thread has been busy (>= 0.5 core) for 5 s in a
# row, then profile that thread with call graphs (12 s) and the whole broker flat (6 s). Text reports to /root/p3/prof/,
# DONE written last.
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
perf record -t "$tid" -F 999 --call-graph dwarf,8192 -o $OUT/planner.data -- sleep 12 2>&1 | tail -2
echo "whole record start $(date -u +%H:%M:%S)"
perf record -p "$pid" -F 499 -o $OUT/all.data -- sleep 6 2>&1 | tail -2
echo "reports $(date -u +%H:%M:%S)"
perf report -i $OUT/planner.data --no-children --sort symbol --stdio -g none --percent-limit 0.3 > $OUT/planner-self.txt 2>&1
perf report -i $OUT/planner.data --children --sort symbol --stdio -g none --percent-limit 0.8 > $OUT/planner-children.txt 2>&1
perf report -i $OUT/planner.data --no-children --sort symbol --stdio -g caller,0.5,callee,function,percent --percent-limit 1.5 > $OUT/planner-callers.txt 2>&1
perf report -i $OUT/all.data --sort comm,symbol --stdio --percent-limit 0.25 > $OUT/all-flat.txt 2>&1
perf report -i $OUT/all.data --sort comm --stdio > $OUT/all-comm.txt 2>&1
echo "done $(date -u +%H:%M:%S)"
touch $OUT/DONE
