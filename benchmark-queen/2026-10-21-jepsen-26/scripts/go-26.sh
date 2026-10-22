#!/usr/bin/env bash
# go-26.sh: the setup check, then (only if it is valid) the main list on n1..n5, with the failover list on n1..n6 and
# the batched-push list on n1..n5 queued behind it, one after the other.
cd /root/jepsen-26
./smoke-26.sh > /root/runs-26-smoke.out 2>&1
if grep -q "done  s-w8-none: valid" /root/runs-26-smoke/summary.txt; then
  mkdir -p /root/runs-26 /root/runs-26-fo
  (setsid nohup ./campaign-26-fo.sh > /root/runs-26-fo.out 2>&1 < /dev/null &)
  (setsid nohup ./campaign-26-batch.sh > /root/runs-26-batch.out 2>&1 < /dev/null &)
  ./campaign-26.sh > /root/runs-26.out 2>&1
else
  echo "$(date -u +%FT%TZ) SETUP CHECK NOT VALID: campaign not started" >> /root/runs-26-smoke/summary.txt
fi
