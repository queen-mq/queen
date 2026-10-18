#!/usr/bin/env bash
# after-25.sh: once the batched-push list has ended (the last of the three):
#   1. p11-w1-corrupt-all once more, on n1..n5 with the same harness and binary (Alice, 10-18: its first run was
#      INVALID on the panic checker alone, NA-QLOG-I1 boot refusals after a deliberate qlog truncation; report both);
#   2. the rolling upgrade 2.4.0 -> 2.5.0 on n1..n3 (it needs nodes no Jepsen test holds);
#   3. the archive.
until grep -q "ALL DONE matrix-24-batch.txt" /root/runs-25-batch/summary.txt 2>/dev/null; do sleep 60; done
sleep 30
cd /root/jepsen-25
grep '^p11-w1-corrupt-all ' matrix-220.txt > matrix-rerun-25.txt
export RUNS=/root/runs-25-rerun BIN=/root/bin/queen-25 NODES=n1,n2,n3,n4,n5
mkdir -p $RUNS; md5sum $BIN > $RUNS/binary.md5
./run-campaign.sh matrix-rerun-25.txt > /root/runs-25-rerun.out 2>&1
echo "$(date -u +%FT%TZ) ALL DONE matrix-rerun-25.txt" >> $RUNS/summary.txt
for n in n1 n2 n3; do ssh -n -o BatchMode=yes $n 'pkill -9 -x queen; true'; done
/root/roll/roll25.sh > /root/roll25.out 2>&1
/root/jepsen-25/collect-jepsen-25.sh > /root/collect-25.out 2>&1
echo "$(date -u +%FT%TZ) AFTER DONE" >> /root/runs-25-batch/summary.txt
