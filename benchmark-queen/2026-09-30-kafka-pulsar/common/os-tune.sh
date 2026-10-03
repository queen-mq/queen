#!/usr/bin/env bash
# os-tune.sh <broker|loader> — the same kernel settings on every box, for every system (Queen included), runtime only
# (nothing persisted: a reboot restores the droplet's defaults). Idempotent; prints every key before -> after and
# SKIPS (with a note) what the kernel refuses, e.g. inside a rehearsal container.
# Why each value: SPEC.md §5 "OS". THP stays at Ubuntu's madvise; no IRQ pinning (it made Queen worse on 09-29).
set -u
ROLE=${1:-broker}
case $ROLE in broker|loader) ;; *) echo "usage: os-tune.sh <broker|loader>"; exit 2;; esac
set_kv() {  # set_kv key value
  local k=$1 v=$2 f=/proc/sys/${1//.//} before after
  [ -r "$f" ] || { echo "  skip $k (absent)"; return; }
  before=$(tr -s '\t ' ' ' < "$f")
  if [ "$before" = "$v" ]; then echo "  $k = $v (already)"; return; fi
  sysctl -qw "$k=$v" 2>/dev/null; after=$(tr -s '\t ' ' ' < "$f")
  if [ "$after" = "$v" ]; then echo "  $k: $before -> $after"
  else echo "  skip $k (not settable here; stays $before)"; fi
}
echo "[$(date -u +%FT%TZ)] os-tune $ROLE on $(hostname) ($(hostname -I 2>/dev/null | awk '{print $1}'))"
set_kv vm.swappiness 1
set_kv vm.max_map_count 4194304
set_kv vm.dirty_background_ratio 5
set_kv vm.dirty_ratio 60
set_kv fs.file-max 4194304
set_kv net.core.somaxconn 4096
set_kv net.ipv4.tcp_max_syn_backlog 8192
set_kv net.core.netdev_max_backlog 32768
set_kv net.core.rmem_max 67108864
set_kv net.core.wmem_max 67108864
set_kv net.ipv4.tcp_rmem "4096 131072 67108864"
set_kv net.ipv4.tcp_wmem "4096 131072 67108864"
set_kv net.ipv4.tcp_slow_start_after_idle 0
if [ "$ROLE" = loader ]; then
  set_kv net.ipv4.ip_local_port_range "10240 65535"
  set_kv net.ipv4.tcp_tw_reuse 1
fi
# nofile for new login sessions (the start scripts also `ulimit -n 1048576` themselves)
if [ -w /etc/security/limits.d ]; then
  printf '* soft nofile 1048576\n* hard nofile 1048576\nroot soft nofile 1048576\nroot hard nofile 1048576\n' > /etc/security/limits.d/90-bench.conf
  echo "  limits.d nofile 1048576 (new sessions)"
fi
# noatime on the filesystem that holds the data (ext4 root on the droplets): no metadata write per read
FS=$(findmnt -no TARGET --target /root 2>/dev/null)
OPTS=$(findmnt -no OPTIONS --target /root 2>/dev/null)
if [ -n "$FS" ] && ! echo ",$OPTS," | grep -q ',noatime,'; then
  if mount -o remount,noatime "$FS" 2>/dev/null; then echo "  $FS remounted noatime"; else echo "  skip noatime on $FS (not permitted here)"; fi
else echo "  $FS noatime (already)"; fi
echo "  THP: enabled=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null) defrag=$(cat /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null) (left as is)"
echo "  cpu: $(nproc) x $(lscpu 2>/dev/null | awk -F': +' '/^Model name/{print $2; exit}')"
