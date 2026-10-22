#!/usr/bin/env bash
# collect-jepsen-26.sh: gather what the Jepsen archive of 2.6 keeps into /root/jepsen-26-archive (summaries, lists,
# checksums, notes, the scripts that ran, each test's results.edn, the setup check, the rolling upgrade; not the stores).
set -u
OUT=/root/jepsen-26-archive; rm -rf $OUT; mkdir -p $OUT/scripts $OUT/upgrade
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
grab /root/runs-26-smoke setup-check
grab /root/runs-26 main
grab /root/runs-26-fo failover
grab /root/runs-26-batch batch
cp /root/jepsen-26/matrix-220.txt $OUT/main/matrix.txt
cp /root/jepsen-26/matrix-220b2.txt $OUT/failover/matrix.txt
cp /root/jepsen-26/matrix-smoke-26.txt $OUT/setup-check/matrix.txt
[ -d $OUT/batch ] && cp /root/jepsen-26/matrix-24-batch.txt $OUT/batch/matrix.txt
cp /root/jepsen-26/after-26.sh /root/jepsen-26/campaign-26.sh /root/jepsen-26/campaign-26-fo.sh /root/jepsen-26/campaign-26-batch.sh /root/jepsen-26/smoke-26.sh /root/jepsen-26/go-26.sh /root/jepsen-26/collect-jepsen-26.sh $OUT/scripts/
cp /root/jepsen-26/project.clj $OUT/scripts/project.clj.as-run
cp /root/roll/roll26.sh /root/roll/node.sh /root/roll/dup.py /root/roll/health.py /root/roll/runs/roll26/roll.log $OUT/upgrade/
md5sum /root/bin/queen-26 /root/bin/queen-250 > $OUT/binaries.md5
du -sh $OUT
