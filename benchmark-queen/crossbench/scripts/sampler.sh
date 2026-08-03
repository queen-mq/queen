#!/usr/bin/env bash
# 1 Hz resource sampler for the CM-BENCH broker VM (SPEC.md §4).
#
# Emits one CSV line per second: per-container CPU cores and RSS read straight
# out of cgroup v2, plus host disk and network deltas. Reading cgroup counters
# directly rather than shelling out to `docker stats` matters: docker stats
# takes about a second to produce a sample, which at a 1 Hz cadence would skew
# every reading it took.
#
#   ./sampler.sh cmbench-pgmq > samples.csv
#   ./sampler.sh cmbench-kafka
#   ./sampler.sh cmbench-queen cmbench-queen-pg      # broker + database
#
# Run it on the broker VM for the whole run, and on the loader VM too: SPEC.md
# §5.1 voids any run where the loader went over 70% CPU, and the only way to
# prove it did not is to have sampled it.
set -euo pipefail

# With no arguments, sample the HOST instead of containers. That is the loader
# VM's mode: SPEC.md §5.1 voids any run where the loader went over 70% CPU, and
# the only way to prove it did not is to have sampled it.
CONTAINERS=("$@")
HOST_MODE=0
if [ ${#CONTAINERS[@]} -eq 0 ]; then
  HOST_MODE=1
fi

CGROUP_ROOT=/sys/fs/cgroup
DISK_DEV="${CM_DISK_DEV:-}"
NET_DEV="${CM_NET_DEV:-}"

# Auto-detect the busiest block device and the default-route interface when the
# operator has not pinned them.
if [ -z "$DISK_DEV" ]; then
  DISK_DEV=$(awk '$3 ~ /^(nvme[0-9]+n[0-9]+|sd[a-z]|vd[a-z])$/ {print $3; exit}' /proc/diskstats || true)
fi
if [ -z "$NET_DEV" ]; then
  NET_DEV=$(ip route show default 2>/dev/null | awk '{print $5; exit}' || true)
fi

# cgroup_path <container> — resolve a container's cgroup v2 directory.
cgroup_path() {
  local name="$1" id
  id=$(docker inspect -f '{{.Id}}' "$name" 2>/dev/null || true)
  [ -z "$id" ] && return 1
  for p in \
    "$CGROUP_ROOT/system.slice/docker-$id.scope" \
    "$CGROUP_ROOT/docker/$id" \
    "$CGROUP_ROOT/system.slice/docker-$id.scope/init"; do
    [ -d "$p" ] && { echo "$p"; return 0; }
  done
  return 1
}

declare -A CG
for c in "${CONTAINERS[@]}"; do
  if path=$(cgroup_path "$c"); then
    CG["$c"]="$path"
  else
    echo "sampler: cannot resolve cgroup for container '$c' — is it running?" >&2
    exit 1
  fi
done

NCPU=$(nproc)

# read_host_cpu -> cumulative busy jiffies (total minus idle and iowait)
read_host_cpu() {
  awk '/^cpu / {idle=$5+$6; total=0; for (i=2; i<=NF; i++) total+=$i; print total-idle; exit}' /proc/stat
}
read_host_mem_mb() {
  awk '/^MemTotal:/ {t=$2} /^MemAvailable:/ {a=$2} END {printf "%.1f", (t-a)/1024}' /proc/meminfo
}

read_cpu_usec() { awk '/^usage_usec/ {print $2}' "$1/cpu.stat" 2>/dev/null || echo 0; }
read_mem_bytes() { cat "$1/memory.current" 2>/dev/null || echo 0; }

read_disk() { # -> "read_sectors write_sectors"
  [ -z "$DISK_DEV" ] && { echo "0 0"; return; }
  awk -v d="$DISK_DEV" '$3 == d {print $6, $10; found=1; exit} END {if (!found) print 0, 0}' /proc/diskstats
}

read_net() { # -> "rx_bytes tx_bytes"
  [ -z "$NET_DEV" ] && { echo "0 0"; return; }
  awk -v d="$NET_DEV:" '$1 == d {print $2, $10; found=1; exit} END {if (!found) print 0, 0}' /proc/net/dev
}

# ---- header -----------------------------------------------------------------
hdr="ts"
if [ "$HOST_MODE" = 1 ]; then
  hdr="$hdr,host_cores,host_cpu_pct,host_used_mb"
else
  for c in "${CONTAINERS[@]}"; do hdr="$hdr,${c}_cores,${c}_rss_mb"; done
fi
hdr="$hdr,disk_read_mbps,disk_write_mbps,net_rx_mbps,net_tx_mbps,load1"
echo "$hdr"

# ---- prime ------------------------------------------------------------------
declare -A PREV_CPU
for c in "${CONTAINERS[@]}"; do PREV_CPU["$c"]=$(read_cpu_usec "${CG[$c]}"); done
PREV_HOST_CPU=$(read_host_cpu)
read -r PREV_DR PREV_DW <<<"$(read_disk)"
read -r PREV_RX PREV_TX <<<"$(read_net)"
PREV_T=$(date +%s.%N)

while sleep 1; do
  NOW_T=$(date +%s.%N)
  DT=$(awk -v a="$NOW_T" -v b="$PREV_T" 'BEGIN{d=a-b; print (d>0)?d:1}')

  line=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  if [ "$HOST_MODE" = 1 ]; then
    cur=$(read_host_cpu)
    # /proc/stat counts in USER_HZ (100 Hz): delta / 100 / elapsed = cores busy.
    cores=$(awk -v a="$cur" -v b="$PREV_HOST_CPU" -v dt="$DT" 'BEGIN{printf "%.3f", (a-b)/100/dt}')
    pct=$(awk -v c="$cores" -v n="$NCPU" 'BEGIN{printf "%.1f", 100*c/n}')
    line="$line,$cores,$pct,$(read_host_mem_mb)"
    PREV_HOST_CPU=$cur
  else
    for c in "${CONTAINERS[@]}"; do
      cur=$(read_cpu_usec "${CG[$c]}")
      prev=${PREV_CPU[$c]}
      # usage_usec is cumulative CPU time: delta / elapsed = cores in use.
      cores=$(awk -v a="$cur" -v b="$prev" -v dt="$DT" 'BEGIN{printf "%.3f", (a-b)/1000000/dt}')
      rss=$(awk -v m="$(read_mem_bytes "${CG[$c]}")" 'BEGIN{printf "%.1f", m/1048576}')
      line="$line,$cores,$rss"
      PREV_CPU["$c"]=$cur
    done
  fi

  read -r DR DW <<<"$(read_disk)"
  read -r RX TX <<<"$(read_net)"
  # diskstats counts 512-byte sectors.
  drm=$(awk -v a="$DR" -v b="$PREV_DR" -v dt="$DT" 'BEGIN{printf "%.2f", (a-b)*512/1048576/dt}')
  dwm=$(awk -v a="$DW" -v b="$PREV_DW" -v dt="$DT" 'BEGIN{printf "%.2f", (a-b)*512/1048576/dt}')
  rxm=$(awk -v a="$RX" -v b="$PREV_RX" -v dt="$DT" 'BEGIN{printf "%.2f", (a-b)/1048576/dt}')
  txm=$(awk -v a="$TX" -v b="$PREV_TX" -v dt="$DT" 'BEGIN{printf "%.2f", (a-b)/1048576/dt}')
  load1=$(awk '{print $1}' /proc/loadavg)

  echo "$line,$drm,$dwm,$rxm,$txm,$load1"

  PREV_DR=$DR; PREV_DW=$DW; PREV_RX=$RX; PREV_TX=$TX; PREV_T=$NOW_T
done
