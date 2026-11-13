#!/usr/bin/env bash
# setup.sh <qload binary> (Mac): the four machines, once.
#   brokers: packages for the build, the local NVMe disks as one RAID0 (ext4, lazy init off) at /root/p3, the
#            harness scripts and node.env, OS settings (os-tune.sh broker), rustup stable;
#   loader:  qload at /root/p3/bin/qload, OS settings (os-tune.sh loader).
# Prints one line a machine. Idempotent enough to run again (an existing /root/p3 mount is kept).
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts.env}"; QLOAD=${1:?the qload binary for linux/amd64}
PEERS="1=${BV[0]}:7400/${BV[0]}:6632,2=${BV[1]}:7400/${BV[1]}:6632,3=${BV[2]}:7400/${BV[2]}:6632"
BROKER_PREP='set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq build-essential pkg-config clang cmake mdadm python3 chrony >/dev/null
if ! findmnt -n /root/p3 >/dev/null; then
  disks=$(ls /dev/disk/by-id/google-local-nvme-ssd-* | sort)
  n=$(echo "$disks" | wc -l)
  if [ "$n" -gt 1 ]; then
    mdadm --create /dev/md0 --level=0 --raid-devices=$n $disks --run --force >/dev/null 2>&1
    dev=/dev/md0
  else
    dev=$disks
  fi
  mkfs.ext4 -q -F -E lazy_itable_init=0,lazy_journal_init=0 $dev
  mkdir -p /root/p3
  mount -o noatime $dev /root/p3
fi
mkdir -p /root/p3/bin /root/p3/logs /root/p3/run /root/p3/runs
if [ ! -x /root/.cargo/bin/cargo ]; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y -q --profile minimal >/dev/null 2>&1
fi'
for i in 0 1 2; do
  ( n=$((i+1)); h=${BP[$i]}
    $SSH root@$h "$BROKER_PREP" || { echo "broker $n: prep FAILED"; exit 1; }
    $SCP "$HERE/qc.sh" "$HERE/netdisk.sh" "$HERE/sampler.py" "$HERE/os-tune.sh" "$HERE/fsync.py" root@$h:/root/p3/
    printf 'NODE=%s\nPRIV=%s\nPEERS="%s"\n' $n ${BV[$i]} "$PEERS" | $SSH root@$h 'cat > /root/p3/node.env'
    $SSH root@$h 'chmod +x /root/p3/*.sh; bash /root/p3/os-tune.sh broker >/root/p3/os-tune.log; echo "broker '$n': $(nproc) cpu, $(findmnt -no SOURCE,SIZE /root/p3), chrony $(systemctl is-active chrony), $(/root/.cargo/bin/cargo --version 2>/dev/null)"' ) &
done
( h=${LP[0]}
  $SSH root@$h 'export DEBIAN_FRONTEND=noninteractive; apt-get update -qq >/dev/null; apt-get install -y -qq python3 chrony >/dev/null; pkill -x qload; mkdir -p /root/p3/bin /root/p3/runs'
  $SCP "$QLOAD" root@$h:/root/p3/bin/qload; $SCP "$HERE/os-tune.sh" root@$h:/root/p3/
  $SSH root@$h 'chmod +x /root/p3/bin/qload; bash /root/p3/os-tune.sh loader >/root/p3/os-tune.log; echo "loader: $(nproc) cpu, chrony $(systemctl is-active chrony), qload flags $(/root/p3/bin/qload -h 2>&1 | grep -c -E -- "^  -(push-wire|push-zstd|ack-counts|pop-frames|txn-kv)")/5"' ) &
wait
