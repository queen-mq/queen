#!/bin/bash
# Broker failover under load: does the cell survive losing a broker, and what
# does it cost the tenants on it?
#
# Run from the workstation. Load runs on the loader VM, the kill happens on the
# cell VM, and the two are correlated by wall clock.
#
# With consistent hashing over two servers, losing one moves ALL of its tenants
# to the survivor at once — so this also exercises the thundering-herd path that
# the fd ceiling exposed earlier.
set -uo pipefail

CELL=46.101.186.250
LOADER=142.93.170.82
OUT=${OUT:-/root/ladder-failover}
KILL_AT=${KILL_AT:-120}      # seconds after launch (load starts ~20s in)
VICTIM=${VICTIM:-cell-broker-a}

echo "=== backend state before ==="
ssh -o BatchMode=yes root@$CELL "curl -s 'http://127.0.0.1:8404/stats;csv' | awk -F, '/broker-/{print \$2, \$18}'"

ssh -o BatchMode=yes root@$LOADER "OUT=$OUT DUR=240 DRAIN=60 WARMUP=0 RUNGS='r5000:4500:450:50' \
  nohup setsid /root/ladder.sh > /root/ladder-failover.log 2>&1 < /dev/null &"
echo "load launched $(date -u +%H:%M:%SZ); killing $VICTIM in ${KILL_AT}s"

sleep "$KILL_AT"
KILL_TS=$(date -u +%H:%M:%S)
ssh -o BatchMode=yes root@$CELL "docker kill $VICTIM >/dev/null 2>&1; echo killed"
echo "### $VICTIM killed at ${KILL_TS}Z"

# Watch how fast HAProxy notices and where traffic goes.
for i in 1 2 3 4 5 6 8 10 15 20 30; do
  sleep 2
  ssh -o BatchMode=yes root@$CELL "printf '%s  ' \$(date -u +%H:%M:%S); \
    curl -s 'http://127.0.0.1:8404/stats;csv' | awk -F, '/broker-/{printf \"%s=%s \", \$2, \$18}'; echo"
done

echo "=== waiting for the run to finish ==="
until ssh -o BatchMode=yes root@$LOADER 'grep -q "LADDER DONE" /root/ladder-failover.log 2>/dev/null'; do sleep 15; done

echo
echo "=== verdict ==="
ssh -o BatchMode=yes root@$LOADER "tail -10 /root/ladder-failover.log"
echo "=== delivery totals (loss is what matters, dups are at-least-once) ==="
ssh -o BatchMode=yes root@$LOADER "for l in idle active noisy; do printf '%-7s ' \$l; \
  grep -E '^     TOTAL' $OUT/r5000-\$l.log 2>/dev/null | head -1; done"
echo "=== haproxy view of the event ==="
ssh -o BatchMode=yes root@$CELL "docker logs cell-lb 2>&1 | grep -iE 'broker-(a|b).*(DOWN|UP)' | tail -6"
echo "### kill was at ${KILL_TS}Z"
