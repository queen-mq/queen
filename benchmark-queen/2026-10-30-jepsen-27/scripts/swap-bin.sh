#!/bin/sh
# Swap the campaign's binary for the next-release build right after the test
# in progress finishes, so no test's setup sees a half-swapped file.
log=/root/build-new.log
sum=/root/runs-prio/summary.txt
new=/root/src/server/target/release/queen
until grep -q '^EXIT ' "$log"; do sleep 10; done
if ! grep -q '^EXIT 0' "$log"; then
  echo "$(date -u +%FT%TZ) note  new build FAILED, binary not swapped" | tee -a /root/swap.log >> "$sum"
  exit 1
fi
cp "$new" /root/bin/queen-nr.next
n=$(grep -c ' done ' "$sum")
while [ "$(grep -c ' done ' "$sum")" -eq "$n" ]; do sleep 1; done
[ -f /root/bin/queen-nr-19f3d492c ] || cp /root/bin/queen-nr /root/bin/queen-nr-19f3d492c
mv /root/bin/queen-nr.next /root/bin/queen-nr
md5=$(md5sum /root/bin/queen-nr | cut -c1-32)
echo "$(date -u +%FT%TZ) note  binary swapped to next-release f9c9a37f2 (md5 $md5) after $(grep -c ' done ' "$sum") tests" | tee -a /root/swap.log >> "$sum"
