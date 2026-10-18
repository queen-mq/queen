#!/usr/bin/env bash
# gapprobe.sh <load start, unix ms> <out dir> : on this broker, one window of a load (W: its first second, default 61,
# after the windows the median statistic reads; LEN: its length, default 6): every thread on the CPU with its stack
# (perf, 499 Hz), where the threads that are not tokio workers wait off the CPU, every file sync by thread, the delay
# from wake-up to running (gap.bt), the CPU of every thread (/proc, before and after), and this node's role.
# 2026-10-14, Queen vs Kafka: where the 20 ms of a 3M msg/s point go. The perf report is written after AFTER seconds
# of the load (default 100), so that it takes no CPU from it.
START=$1; OUT=$2; mkdir -p "$OUT"; PID=$(pgrep -x queen | head -1)
W=${W:-61}; LEN=${LEN:-6}
for t in /proc/$PID/task/*; do echo "$(basename $t) $(tr ' ' _ < $t/comm)"; done > "$OUT/threads"
python3 -c "import time; print(time.time() - time.clock_gettime(time.CLOCK_MONOTONIC))" > "$OUT/wall-minus-mono"
cpu() { # utime + stime ticks of every thread
  for t in /proc/$PID/task/*; do s=($(cut -d')' -f2 $t/stat 2>/dev/null)); echo "$(basename $t) $(tr ' ' _ < $t/comm) ${s[11]} ${s[12]}"; done > "$OUT/cpu-$1"
  date +%s.%N > "$OUT/cpu-$1.t"
}
sleep "$(python3 -c "import time; print(max(0.0, $START / 1000 + $W - time.time()))")"
curl -s -m 2 "http://$(hostname -I | awk '{print $1}'):6632/health" > "$OUT/health" 2>/dev/null
bpftrace /root/p3/gap.bt "$PID" > "$OUT/gap.txt" 2>&1 &
B=$!
sleep 1
cpu a
# FLAT=1 (2026-10-14): no call stacks (the dwarf stacks of the 2.3.0 release binary came out empty on these VMs),
# 999 Hz, and a report by thread name and function.
CG="--call-graph dwarf,8192"; HZ_=499; [ -n "${FLAT:-}" ] && { CG=""; HZ_=999; }
perf record -q -k CLOCK_MONOTONIC -o /dev/shm/gp.data -F $HZ_ -e cpu-clock $CG -p "$PID" -- sleep "$LEN" > "$OUT/perf.log" 2>&1
cpu b
kill -INT $B; wait $B 2>/dev/null
sleep "$(python3 -c "import time; print(max(0.0, $START / 1000 + ${AFTER:-100} - time.time()))")"
if [ -n "${FLAT:-}" ]; then
  perf report -i /dev/shm/gp.data --no-children --sort comm,sym --stdio --percent-limit 0.2 2>>"$OUT/perf.log" | grep -v '^$' | head -400 > "$OUT/perf-flat.txt"
else
  perf script -i /dev/shm/gp.data -F comm,tid,time,ip,sym --no-inline 2>>"$OUT/perf.log" | gzip > "$OUT/stacks.txt.gz"
fi
ls -la /dev/shm/gp.data >> "$OUT/perf.log" 2>&1; rm -f /dev/shm/gp.data
echo PROBE_DONE > "$OUT/done"
