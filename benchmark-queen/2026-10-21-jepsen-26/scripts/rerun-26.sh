#!/usr/bin/env bash
# rerun-26.sh (2026-10-22): pause the failover list, rerun p11-w5-slow-lz-kill three times on the current
# binary (2.6.0-beta.6), then resume the failover list with the tests it has not finished; the batch list
# and after-26.sh (rolling upgrade + collect) keep waiting on the failover list's ALL DONE line, as before.
set -u
cd /root/jepsen-26
FO=/root/runs-26-fo
note() { echo "$(date -u +%FT%TZ) $*" | tee -a /root/rerun-26.log; }

# 1. The failover script first, so it cannot write ALL DONE when its campaign dies.
for p in $(pgrep -f "[c]ampaign-26-fo.sh"); do kill -TERM "$p"; done
# 2. Its campaign, and the test running now (a process group of its own).
for p in $(pgrep -f "[r]un-campaign.sh matrix-220b2.txt"); do kill -TERM "$p"; done
for p in $(pgrep -f "[l]ein run test --nodes n1,n2,n3,n4,n5,n6"); do kill -TERM -- "-$p" 2>/dev/null; done
sleep 30
for p in $(pgrep -f "[l]ein run test"); do kill -KILL -- "-$p" 2>/dev/null; done
for n in n1 n2 n3 n4 n5 n6; do ssh -n -o BatchMode=yes "$n" 'pkill -9 -x queen; true'; done
cur=$(grep " start " "$FO/summary.txt" | tail -1 | sed -E 's/.* start ([^:]+):.*/\1/')
echo "$(date -u +%FT%TZ) paused $cur for the rerun of p11-w5-slow-lz-kill (rerun-26.sh); it runs again on resume" >> "$FO/summary.txt"
note "failover list paused during $cur"

# 3. The reruns, on n1..n5 like the main list.
line=$(grep '^p11-w5-slow-lz-kill ' matrix-220.txt | sed -E 's/^[^ ]+ +//')
: > matrix-rerun-26.txt
for i in 1 2 3; do echo "rr$i-p11-w5-slow-lz-kill $line" >> matrix-rerun-26.txt; done
note "rerun line: $line"
RUNS=/root/runs-26-rerun BIN=/root/bin/queen-26 NODES=n1,n2,n3,n4,n5 ./run-campaign.sh matrix-rerun-26.txt
note "reruns finished: $(grep ' done ' /root/runs-26-rerun/summary.txt | sed -E 's/.* done +([^:]+): ([A-Za-z]+).*/\1=\2/' | tr '\n' ' ')"

# 4. Resume the failover list without the tests it finished.
grep ' done ' "$FO/summary.txt" | sed -E 's/.* done +([^:]+):.*/\1/' > /root/fo-done-labels.txt
awk 'NR==FNR {d[$1]=1; next} /^#/ || NF==0 {next} !($1 in d)' /root/fo-done-labels.txt matrix-220b2.txt > matrix-220b2-rest.txt
note "failover list resumes with $(wc -l < matrix-220b2-rest.txt) tests"
RUNS=$FO BIN=/root/bin/queen-26 NODES=n1,n2,n3,n4,n5,n6 ./run-campaign.sh matrix-220b2-rest.txt
echo "$(date -u +%FT%TZ) ALL DONE matrix-220b2.txt" >> "$FO/summary.txt"
note "failover list done"
