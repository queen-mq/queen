#!/usr/bin/env bash
# after-omb.sh: once the OMB matrix's last chain (Kafka's backlog reruns, ../2026-10-16-omb/runs/matrix-kbacklog.log)
# has ended, stop what it left on the VMs (OMB workers, Kafka, Queen), then run ab.sh.
H=/Users/alice/Work/queen/benchmark-queen/2026-10-16-omb
until grep -q "matrix: all points done" $H/runs/matrix-kbacklog.log 2>/dev/null; do sleep 30; done
sleep 10
echo "[$(date -u +%T)] OMB matrix done; cleaning the VMs" >> ab.out
bash $H/scripts/omb.sh stop-all < /dev/null >> ab.out 2>&1
for b in <host-37> <host-34> <host-35>; do
  ssh -n -o BatchMode=yes root@$b "pkill -9 -f '[k]afka.Kafka'; /root/p3/qc.sh stop > /dev/null 2>&1; pkill -9 -x queen; true" &
done; wait
echo "[$(date -u +%T)] VMs clean; A/B starts" >> ab.out
bash ./ab.sh
