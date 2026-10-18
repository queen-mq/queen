#!/bin/bash
# prep.sh — brokers: RAID0 of the four local NVMe disks, XFS, noatime at /root/p3, fstrim, /root/bench bind-mounted
# from /root/p3/bench (as on 10-14/16), os-tune broker; loaders: os-tune loader, Java 17 (the benchmark's JVM).
set -u
G=$(cd "$(dirname "$0")" && pwd); . $G/../../hosts.env
COMMON=/Users/alice/Work/queen/benchmark-queen/2026-09-30-kafka-pulsar/common
DISK='set -e
export DEBIAN_FRONTEND=noninteractive
command -v mdadm >/dev/null && command -v mkfs.xfs >/dev/null || { apt-get update -qq >/dev/null && apt-get install -y -qq mdadm xfsprogs >/dev/null; }
if ! mountpoint -q /root/p3; then
  DEVS=$(ls /dev/disk/by-id/google-local-nvme-ssd-* | sort)
  mdadm --create /dev/md0 --level=0 --raid-devices=4 --run $DEVS >/dev/null 2>&1
  mkfs.xfs -f -q /dev/md0
  mkdir -p /root/p3 && mount -o noatime /dev/md0 /root/p3
fi
fstrim /root/p3 || true
mkdir -p /root/p3/bench /root/bench
mountpoint -q /root/bench || mount --bind /root/p3/bench /root/bench
echo "$(hostname -s): $(findmnt -no SOURCE,FSTYPE /root/p3) $(df -h --output=size /root/p3 | tail -1) bench=$(findmnt -no SOURCE /root/bench)"'
for h in "${B_PUB[@]}"; do
  ( for t in $(seq 1 30); do $SSH root@$h true 2>/dev/null && break; sleep 4; done
    $SSH root@$h "$DISK" && $SSH root@$h 'mkdir -p /root/bench/common' && $SCP $COMMON/*.sh root@$h:/root/bench/common/ \
    && $SCP $G/../../hosts.env root@$h:/root/bench/hosts.env && $SSH root@$h 'bash /root/bench/common/os-tune.sh broker >/dev/null; echo "$(hostname -s): os-tune ok"' ) &
done
for h in "${L_PUB[@]}"; do
  ( for t in $(seq 1 30); do $SSH root@$h true 2>/dev/null && break; sleep 4; done
    $SSH root@$h 'mkdir -p /root/bench/common' && $SCP $COMMON/*.sh root@$h:/root/bench/common/ && $SCP $G/../../hosts.env root@$h:/root/bench/hosts.env \
    && $SSH root@$h 'export DEBIAN_FRONTEND=noninteractive; bash /root/bench/common/os-tune.sh loader >/dev/null; apt-get update -qq >/dev/null; apt-get install -y -qq openjdk-17-jre-headless >/dev/null 2>&1; echo "$(hostname -s): $(java -version 2>&1 | head -1) nproc=$(nproc)"' ) &
done
wait
