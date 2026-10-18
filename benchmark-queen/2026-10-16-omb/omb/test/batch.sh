#!/usr/bin/env bash
# batch.sh <list>: runs each "name driver workload shards disk|shm" line in turn (fresh broker each), collects it.
HERE=$(cd "$(dirname "$0")" && pwd)
exec 3< "$1"
while read -r name drv wl sh where <&3; do
  [ -z "$name" ] && continue; case $name in \#*) continue;; esac
  echo "=== $(date -u +%T) $name"
  bash $HERE/vm.sh broker-start $sh $where < /dev/null | cut -c1-60
  bash $HERE/run.sh start $name $HERE/$drv $HERE/$wl < /dev/null
  n=0; until bash $HERE/run.sh done $name < /dev/null 2>/dev/null; do sleep 10; n=$((n+1)); [ $n -gt 90 ] && { echo "TIMEOUT $name"; break; }; done
  bash $HERE/run.sh collect $name < /dev/null > $HERE/runs/$name.collect 2>&1
  grep -E "driver closed|Queen verify" $HERE/runs/$name/worker-808*.slice | grep -v " 0 subscriptions, 0 partitions" | sed -E 's/^.{0,120}(INFO|WARN) QueenBenchmarkDriver - //' | cut -c1-420
  python3 $HERE/summary.py $HERE/runs/$name
done
echo "=== $(date -u +%T) batch done"
