#!/usr/bin/env bash
# S2 — FAILURE MODE. What does a client actually SEE at and beyond saturation?
#
# Every point captures, for the same seconds:
#   * the client's view — errors by kind and by HTTP code, Retry-After presence
#     and value, latency percentiles, achieved vs offered
#   * the correctness view — per-message bitmaps: loss / duplication / extra /
#     cross-tenant. An overloaded broker that silently drops is a different
#     product from one that 429s, so this is measured at EVERY overload point.
#   * the server's view — the broker/proxy log slices produced during the run
#     (byte offsets taken before and after), systemd unit state, dmesg tail.
#
# Overload points use a SHORT load phase and a LONG drain: the backlog an
# overload builds must be given time to come out, otherwise "still queued" is
# reported as "lost" and the whole question is answered wrongly.
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=/root/campaign/S/S2
mkdir -p $OUT
cp /root/campaign/S/tenants.json $OUT/tenants.json 2>/dev/null
cp /root/campaign/S/tenants-free.json $OUT/tenants-free.json 2>/dev/null
SHAPE="cell UNCAPPED (cpu.max=max) 8 vCPU/15GiB; broker=M d78709a3 proxy=e0034cb9; PGSS=0"
BASE="-mode cloud -shared-queue -queue orders -group workers -partitions 8 -push-batch 1 -producers-per-tenant 3 -consumers-per-tenant 4 -pop-batch 100 -pop-wait -payload 256 -fail-on-verify=false -out $OUT"

point() { # runid rate duration drain tenantsfile ntenants extra...
  local id=$1 rate=$2 dur=$3 drain=$4 tf=$5 nt=$6; shift 6
  bash $G/reset-cell-db.sh >/dev/null 2>&1
  local bo po
  bo=$(stat -c%s /root/cell/broker.log); po=$(stat -c%s /root/cell/proxy.log)
  bash $G/runpt.sh $OUT "$id" -- $BASE -target proxy -tenants-file "$tf" -tenants "$nt" \
    -rate "$rate" -duration "$dur" -drain "$drain" -run-id "$id" \
    -note "S2 $SHAPE; offered=$rate dur=${dur}s drain=${drain}s; $*" "$@" \
    >/dev/null 2>&1
  local rc=$?
  # the server's own account of the same seconds
  tail -c +$((bo+1)) /root/cell/broker.log >"$OUT/$id.broker.log"
  tail -c +$((po+1)) /root/cell/proxy.log  >"$OUT/$id.proxy.log"
  {
    echo "=== $id  units ==="
    systemctl show queen-broker -p ActiveState -p SubState -p NRestarts -p MemoryCurrent
    systemctl show queen-proxy  -p ActiveState -p SubState -p NRestarts -p MemoryCurrent
    echo "=== docker ==="; docker ps --format '{{.Names}} {{.Status}}' | grep cell- || true
    echo "=== broker log: level counts ==="
    grep -oE ' (ERROR|WARN|INFO) ' "$OUT/$id.broker.log" | sort | uniq -c
    echo "=== broker log: non-INFO lines (first 40) ==="
    grep -E ' (ERROR|WARN) ' "$OUT/$id.broker.log" | head -40
    echo "=== proxy log: level counts ==="
    grep -oE ' (ERROR|WARN|INFO) ' "$OUT/$id.proxy.log" | sort | uniq -c
    echo "=== proxy log: non-INFO lines (first 40) ==="
    grep -E ' (ERROR|WARN) ' "$OUT/$id.proxy.log" | head -40
    echo "=== dmesg tail ==="; dmesg -T 2>/dev/null | tail -15
    echo "=== oom ==="; dmesg -T 2>/dev/null | grep -ci "out of memory\|oom-kill" || true
  } >"$OUT/$id.evidence.txt" 2>&1
  echo "  [$(date -u +%T)] $id exit=$rc"
}

echo "=== S2a: past the knee, through the proxy, bench plan (no proxy limiter) ==="
# short load + long drain: an overload's backlog must be allowed to come out
point s2-ov-4000  4000  30 150 $OUT/tenants.json 8
point s2-ov-8000  8000  30 150 $OUT/tenants.json 8
point s2-ov-16000 16000 30 180 $OUT/tenants.json 8

echo "=== S2b: FREE plan driven at ~40x its limit — is the refusal honest? ==="
# free = 5 req/s, 20 msg/s, 50 parked pops per tenant
point s2-free-800 800 45 60 $OUT/tenants-free.json 4
point s2-free-80   80 45 60 $OUT/tenants-free.json 4

echo "=== S2c: does it come back? baseline rate immediately after the worst overload ==="
point s2-recover-1000 1000 45 45 $OUT/tenants.json 8

echo "=== S2 done $(date -u +%T) ==="
