#!/usr/bin/env bash
# after-26.sh: once the batched-push list has ended (the last of the three): the rolling upgrade 2.5.0 -> 2.6 and back
# on n1..n3 (it needs nodes no Jepsen test holds), then the archive.
until grep -q "ALL DONE matrix-24-batch.txt" /root/runs-26-batch/summary.txt 2>/dev/null; do sleep 60; done
sleep 30
for n in n1 n2 n3; do ssh -n -o BatchMode=yes $n 'pkill -9 -x queen; true'; done
/root/roll/roll26.sh > /root/roll26.out 2>&1
/root/jepsen-26/collect-jepsen-26.sh > /root/collect-26.out 2>&1
echo "$(date -u +%FT%TZ) AFTER DONE" >> /root/runs-26-batch/summary.txt
