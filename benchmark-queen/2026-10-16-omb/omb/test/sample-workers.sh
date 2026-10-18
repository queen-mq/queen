#!/usr/bin/env bash
# on B: every 5 s, each OMB worker JVM's RSS, threads, CPU ticks, and the TCP connections to the broker; until $1/DONE
d=$1
while [ ! -f $d/DONE ]; do
  for p in $(pgrep -f 'io.openmessaging.benchmark.worker.BenchmarkWorker'); do
    port=$(tr '\0' ' ' < /proc/$p/cmdline | grep -o -- '--port [0-9]*' | awk '{print $2}')
    echo "$(date -u +%T) port $port rss_mb $(( $(awk '/VmRSS/{print $2}' /proc/$p/status) / 1024 )) threads $(ls /proc/$p/task | wc -l) cpu_ticks $(awk '{print $14+$15}' /proc/$p/stat)"
  done
  echo "$(date -u +%T) tcp_to_broker $(ss -tn state established '( dport = :6632 )' | tail -n +2 | wc -l)"
  sleep 5
done
