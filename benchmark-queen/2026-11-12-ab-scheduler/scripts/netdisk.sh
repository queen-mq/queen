#!/usr/bin/env bash
# Once a second: wall time, bytes received and sent on the private interface, and the root disk's counters from
# /sys/block/<dev>/stat: sectors written (wsec), write requests and the ms they took (wios, wticks: wticks / wios over
# an interval is the device's mean write latency), ms the device was busy (ioticks) and the queue's weighted ms (tiq),
# discards and flushes with their ms (dios, dticks, fios, fticks).
. /root/p3/node.env
IF=$(ip -o -4 addr show | awk -v a="$PRIV/" 'index($4, a) == 1 {print $2; exit}')
# The device that holds /root/p3: its own (/dev/md0 on GCP, a RAID of the local NVMe disks), or the disk under its partition (vda on DO).
SRC=$(findmnt -no SOURCE --target /root/p3); DEV=$(basename "$(readlink -f "$SRC")")
[ -e /sys/block/$DEV/stat ] || DEV=$(lsblk -no PKNAME "$SRC" 2>/dev/null | head -1); DEV=${DEV:-vda}
while true; do
  echo "t=$(date +%s.%N) rx=$(cat /sys/class/net/$IF/statistics/rx_bytes) tx=$(cat /sys/class/net/$IF/statistics/tx_bytes) $(awk '{printf "wsec=%s wios=%s wticks=%s ioticks=%s tiq=%s dios=%s dticks=%s fios=%s fticks=%s", $7, $5, $8, $10, $11, $12, $15, $16, $17}' /sys/block/$DEV/stat)"
  sleep 1
done
