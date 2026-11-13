#!/usr/bin/env bash
# profcmp.sh <build> [rate=3250000] (Mac): one push step at <rate> msg/s (the settings of abplan-homes.sh's push
# ladder), with a flat perf profile of the leader (perf-n1d.sh, any build). Reports to ../runs/profcmp-<build>/,
# PROFCMP_DONE on stdout at the end. Compare two builds with profdiff.py.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  . "$HOSTS_ENV"
  BUILD=${1:?build}; RATE=${2:-3250000}
  OUT=$HERE/../runs/profcmp-$BUILD
  mkdir -p "$OUT"
  $SCP "$HERE/perf-n1d.sh" root@"${BP[0]}":/root/p3/perf-n1d.sh
  $SSH root@"${BP[0]}" "nohup bash /root/p3/perf-n1d.sh > /root/p3/perf-n1.log 2>&1 </dev/null &"
  env PARTS=1000 CONS=17 PROCS=8 SPREAD=1 bash "$HERE/ab.sh" profcmp-$BUILD $BUILD push 40 $RATE > "$OUT/ab.console" 2>&1
  for i in $(seq 1 120); do
    $SSH root@"${BP[0]}" "test -f /root/p3/prof/DONE" && break
    sleep 5
  done
  $SCP root@"${BP[0]}":"/root/p3/prof/*.txt" root@"${BP[0]}":/root/p3/perf-n1.log "$OUT/"
  grep -E "^\[..:..:..\] +[0-9,]+:" "$OUT/ab.console" | tail -1
  echo PROFCMP_DONE $BUILD
}
main "$@"; exit $?
