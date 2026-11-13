#!/usr/bin/env bash
# bins.sh (Mac): hand the two broker binaries of ../bin (built by builds.sh on the first rig) to the brokers of
# HOSTS_ENV, as /root/p3/bin/281/queen and /root/p3/bin/sched/queen, and print their md5s.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts.env}"
for i in 0 1 2; do
  ( $SSH root@${BP[$i]} 'mkdir -p /root/p3/bin/281 /root/p3/bin/sched'
    $SCP "$HERE/../bin/281/queen" root@${BP[$i]}:/root/p3/bin/281/queen
    $SCP "$HERE/../bin/sched/queen" root@${BP[$i]}:/root/p3/bin/sched/queen
    $SSH root@${BP[$i]} 'chmod +x /root/p3/bin/*/queen; echo "n'$((i+1))': $(md5sum /root/p3/bin/281/queen | cut -c1-8) $(md5sum /root/p3/bin/sched/queen | cut -c1-8)"' ) &
done
wait
