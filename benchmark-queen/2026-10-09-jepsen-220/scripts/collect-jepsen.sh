#!/usr/bin/env bash
# collect-jepsen.sh: gather what the Jepsen archive keeps into /root/jepsen-220-archive
# (summaries, probes, checksums, matrices, each test's results.edn; not the stores).
set -u
OUT=/root/jepsen-220-archive; rm -rf $OUT; mkdir -p $OUT
grab() { # grab <runs dir> <name>
  local RUNS=$1 D=$OUT/$2; mkdir -p $D/results
  cp $RUNS/summary.txt $D/summary.txt
  for f in PROGRESS.txt probe.txt binary.md5 TREE.txt; do [ -e $RUNS/$f ] && cp $RUNS/$f $D/; done
  grep " done " $RUNS/summary.txt | while read -r _ _ label rest; do
    label=${label%:}
    store=$(echo "$rest" | sed -n 's/.*store=\([^ ]*\) log=.*/\1/p')
    [ -n "$store" ] && [ -e "$store/results.edn" ] && cp "$store/results.edn" "$D/results/$label.edn"
  done
  echo "$2: $(grep -c " done " $D/summary.txt) done, $(ls $D/results | wc -l) results, $(grep " done " $D/summary.txt | grep -vc ": valid") not valid"
}
grab /root/runs-rows first-pass
grab /root/runs-220a set-a
grab /root/runs-220b set-b
cp /root/jepsen-220a/matrix-220a.txt $OUT/set-a/matrix.txt
cat /root/jepsen-220b/matrix-220b1.txt /root/jepsen-220b/matrix-220b2.txt /root/jepsen-220b/matrix-220b3.txt > $OUT/set-b/matrix.txt
cp /root/jepsen-rows/matrix-rows.txt $OUT/first-pass/matrix.txt 2>/dev/null
# The four tests of the first pass that were not valid: the lines that say why.
R=$OUT/first-pass/not-valid.txt; : > $R
grep " done " /root/runs-rows/summary.txt | grep -v ": valid" | while read -r _ _ label rest; do
  label=${label%:}; store=$(echo "$rest" | sed -n 's/.*store=\([^ ]*\) log=.*/\1/p')
  echo "== $label" >> $R
  for n in n1 n2 n3 n4 n5; do
    f=$(ls $store/$n/queen.log 2>/dev/null | head -1)
    [ -n "$f" ] && grep -h -m 3 -E "FATAL|refusing|are not all in the queue logs|it ends at seq" "$f" | cut -c1-400 | sed "s/^/$n: /" >> $R
  done
done
md5sum /root/bin/queen-rows /root/bin/queen-220rc1.orig /root/bin/queen-220rc3 /root/bin/queen-220rc4 /root/bin/queen-220rc5 /root/kvl/bin/queen > $OUT/binaries.md5
du -sh $OUT
