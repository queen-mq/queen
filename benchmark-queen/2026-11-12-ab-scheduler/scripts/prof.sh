#!/usr/bin/env bash
# prof.sh (Mac): one push step at 3M msg/s on sched3 (same settings as abplan-fix3.sh) with perf-n1.sh profiling n1's
# planner thread. Reports to ../runs/prof-push-sched3/, PROF_DONE on stdout at the end.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  . "$HOSTS_ENV"
  OUT=$HERE/../runs/prof-push-sched3
  mkdir -p "$OUT"
  $SCP "$HERE/perf-n1.sh" root@"${BP[0]}":/root/p3/perf-n1.sh
  $SSH root@"${BP[0]}" "nohup bash /root/p3/perf-n1.sh > /root/p3/perf-n1.log 2>&1 </dev/null &"
  env PARTS=1000 CONS=17 PROCS=8 bash "$HERE/ab.sh" prof-push-sched3 sched3 push 40 3000000 > "$OUT/ab.console" 2>&1
  for i in $(seq 1 120); do
    $SSH root@"${BP[0]}" "test -f /root/p3/prof/DONE" && break
    sleep 5
  done
  $SCP root@"${BP[0]}":"/root/p3/prof/*.txt" root@"${BP[0]}":/root/p3/perf-n1.log "$OUT/"
  grep -E "^\[..:..:..\] +[0-9,]+:" "$OUT/ab.console" | tail -1
  echo PROF_DONE
}
main "$@"; exit $?
