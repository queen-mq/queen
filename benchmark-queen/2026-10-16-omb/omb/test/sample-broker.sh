#!/usr/bin/env bash
# on A: every 5 s, the broker's RSS, threads, fds, CPU ticks, data size; until $1 exists (at most ~3 h)
stop=$1; n=0
while [ ! -f $stop ] && [ $n -lt 2000 ]; do n=$((n+1))
  p=$(pgrep -x queen-nr0 | head -1)
  [ -n "$p" ] && echo "$(date -u +%T) rss_mb $(( $(awk '/VmRSS/{print $2}' /proc/$p/status) / 1024 )) threads $(ls /proc/$p/task | wc -l) fds $(ls /proc/$p/fd | wc -l) cpu_ticks $(awk '{print $14+$15}' /proc/$p/stat) data_mb $(du -sm /root/qdata/ | cut -f1)"
  sleep 5
done
