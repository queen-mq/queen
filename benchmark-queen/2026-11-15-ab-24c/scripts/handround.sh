#!/usr/bin/env bash
# handround.sh (controller qab-l1): wait for builds.sh's two builds, then give both binaries to all three brokers as
# /root/p3/bin/<281|new>/queen (scp -3 through here: the brokers do not hold each other's keys), keep a copy in
# /root/p3/bin/<name>/queen here, and print the md5s.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts.env}"
for spec in 0:281 1:new; do
  i=${spec%%:*}; name=${spec#*:}
  for t in $(seq 1 240); do
    $SSH root@${BP[$i]} "test -f /root/p3/build-$name.done -o -f /root/p3/build-$name.failed" && break; sleep 10
  done
  $SSH root@${BP[$i]} "cat /root/p3/build-$name.done /root/p3/build-$name.failed 2>/dev/null"
  $SSH root@${BP[$i]} "test -f /root/p3/build-$name.done" || { echo "BUILD $name FAILED"; exit 1; }
  mkdir -p /root/p3/bin/$name
  $SCP root@${BP[$i]}:/root/p3/build-$name/server/target/release/queen /root/p3/bin/$name/queen
  for j in 0 1 2; do
    ( $SSH root@${BP[$j]} "mkdir -p /root/p3/bin/$name"
      $SCP /root/p3/bin/$name/queen root@${BP[$j]}:/root/p3/bin/$name/queen
      $SSH root@${BP[$j]} "chmod +x /root/p3/bin/$name/queen; echo b$((j+1)) $name: \$(md5sum /root/p3/bin/$name/queen | cut -c1-8)" ) &
  done
  wait
done
echo HANDROUND_DONE
