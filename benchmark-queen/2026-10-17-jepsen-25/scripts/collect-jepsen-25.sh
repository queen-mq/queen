#!/usr/bin/env bash
# collect-jepsen-25.sh: gather what the Jepsen archive of 2.5.0 keeps into /root/jepsen-25-archive (summaries, lists,
# checksums, notes, the scripts that ran, each test's results.edn, the setup check, the rolling upgrade; not the stores).
set -u
OUT=/root/jepsen-25-archive; rm -rf $OUT; mkdir -p $OUT/scripts $OUT/upgrade
grab() { # grab <runs dir> <name>
  local RUNS=$1 D=$OUT/$2; [ -e $RUNS/summary.txt ] || return 0; mkdir -p $D/results
  cp $RUNS/summary.txt $D/summary.txt
  for f in binary.md5 reanalyze.txt notes.txt cv.txt; do [ -e $RUNS/$f ] && cp $RUNS/$f $D/; done
  grep " done " $RUNS/summary.txt | while read -r _ _ label rest; do
    label=${label%:}
    store=$(echo "$rest" | sed -n 's/.*store=\([^ ]*\) log=.*/\1/p')
    [ -n "$store" ] && [ -e "$store/results.edn" ] && cp "$store/results.edn" "$D/results/$label.edn"
  done
  echo "$2: $(grep -c " done " $D/summary.txt) done, $(ls $D/results | wc -l) results, $(grep " done " $D/summary.txt | grep -vc ": valid") not valid in summary.txt"
}
grab /root/runs-25-smoke setup-check
grab /root/runs-25 main
grab /root/runs-25-fo failover
grab /root/runs-25-batch batch
grab /root/runs-25-rerun rerun
cp /root/jepsen-25/matrix-220.txt $OUT/main/matrix.txt
cp /root/jepsen-25/matrix-220b2.txt $OUT/failover/matrix.txt
cp /root/jepsen-25/matrix-smoke-25.txt $OUT/setup-check/matrix.txt
[ -d $OUT/batch ] && cp /root/jepsen-25/matrix-24-batch.txt $OUT/batch/matrix.txt
[ -d $OUT/rerun ] && cp /root/jepsen-25/matrix-rerun-25.txt $OUT/rerun/matrix.txt
cp /root/jepsen-25/after-25.sh /root/jepsen-25/campaign-25.sh /root/jepsen-25/campaign-25-fo.sh /root/jepsen-25/campaign-25-batch.sh /root/jepsen-25/smoke-25.sh /root/jepsen-25/go-25.sh /root/jepsen-25/collect-jepsen-25.sh $OUT/scripts/
cp /root/jepsen-25/project.clj $OUT/scripts/project.clj.as-run
cp /root/roll/roll25.sh /root/roll/node.sh /root/roll/dup.py /root/roll/health.py /root/roll/runs/roll25/roll.log $OUT/upgrade/
md5sum /root/bin/queen-25 /root/bin/queen-240 > $OUT/binaries.md5
du -sh $OUT
