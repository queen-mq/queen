#!/usr/bin/env bash
# go-25.sh: the setup check, then (only if it is valid) the main list on n1..n5, with the failover list on n1..n6 and
# the batched-push list on n1..n5 queued behind it, one after the other.
cd /root/jepsen-25
./smoke-25.sh > /root/runs-25-smoke.out 2>&1
if grep -q "done  s-w8-none: valid" /root/runs-25-smoke/summary.txt; then
  mkdir -p /root/runs-25 /root/runs-25-fo
  (setsid nohup ./campaign-25-fo.sh > /root/runs-25-fo.out 2>&1 < /dev/null &)
  (setsid nohup ./campaign-25-batch.sh > /root/runs-25-batch.out 2>&1 < /dev/null &)
  ./campaign-25.sh > /root/runs-25.out 2>&1
else
  echo "$(date -u +%FT%TZ) SETUP CHECK NOT VALID: campaign not started" >> /root/runs-25-smoke/summary.txt
fi
