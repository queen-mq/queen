#!/usr/bin/env bash
# Wait for the measured dispatch of RUN_ID, skip WARM seconds, then profile the
# broker for SECONDS with perf (DWARF call graphs) and save per-thread CPU.
# Usage: capture.sh OUT RUN_ID WARM SECONDS
set -euo pipefail
out="$1"; run_id="$2"; warm="$3"; seconds="$4"
mkdir -p "$out"
# grep -c reads all input: grep -q would close the pipe early and, under
# pipefail, fail the producer side.
until [ "$(docker ps --format '{{.Names}}' | grep -c -- '-broker-1$')" -gt 0 ] \
    && [ "$(ps -ax -o args | grep -c "[b]ench:dispatch.*--run-id=${run_id} ")" -gt 0 ]; do sleep 0.5; done
broker=$(docker ps --format '{{.Names}}' | grep -- '-broker-1$' | head -1)
pid=$(docker inspect --format '{{.State.Pid}}' "$broker")
echo "broker $broker pid $pid; waiting ${warm}s" >&2
sleep "$warm"
docker run --rm --privileged --pid=host -v "$out:/out" qprof-perf sh -c "
  cat /proc/$pid/task/*/stat | awk '{print \$2, \$14+\$15}' > /out/threads.before
  perf record -q -F 499 --call-graph dwarf,16384 -p $pid -o /out/perf.data -- sleep $seconds
  cat /proc/$pid/task/*/stat | awk '{print \$2, \$14+\$15}' > /out/threads.after
  perf report -i /out/perf.data --no-children --sort comm --stdio 2>/dev/null > /out/by-thread.txt
  perf report -i /out/perf.data --no-children --sort symbol --stdio -g none --percent-limit 0.3 2>/dev/null > /out/by-symbol.txt
  perf report -i /out/perf.data --children --sort symbol --stdio -g none --percent-limit 1 2>/dev/null > /out/by-symbol-children.txt
"
echo "done: $out" >&2
