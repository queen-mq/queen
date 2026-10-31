#!/bin/bash
# absum.sh: per run of ab.sh, the step line (pushed, latency, cores) and each broker's disk write MB/s and bytes per
# message over the step's seconds 30-60 (netdisk_rate.py of ../2026-10-15-bytes-per-msg).
R=$(cd "$(dirname "$0")/../2026-10-12-three-node-new-forms/runs" && pwd)
N=$(cd "$(dirname "$0")/../2026-10-15-bytes-per-msg/scripts" && pwd)/netdisk_rate.py
for t in "$@"; do
  d=$R/$t; [ -f $d/run.log ] || continue
  step=$(ls -d $d/*/ 2>/dev/null | grep -E '/[0-9]+/$' | head -1); rate=$(basename "$step")
  echo "== $t"
  grep -h "$(printf "%'d" $rate 2>/dev/null || echo $rate):" $d/run.log | sed 's/^.*: offered/  offered/' | cut -c1-300
  python3 $N $rate 30 60 $step/net-n1.log $step/net-n2.log $step/net-n3.log 2>/dev/null | sed 's#^.*/net-#  #'
done
