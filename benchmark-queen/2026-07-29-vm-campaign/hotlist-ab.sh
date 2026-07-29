#!/usr/bin/env bash
# hotlist-ab.sh <outdir> <rate> — is the in-memory candidate ring the stall?
#
# THE HYPOTHESIS, from the evidence and the source:
#   * through a 29s stall the partition had NO lease (worker_id NULL,
#     lease_expires_at NULL) and 2616 messages past `committed`, so the SQL
#     candidate filter's lease term did not hide it;
#   * queen.log_has_pending_v1 has no watermark term at all — it is a plain
#     `last_offset > committed` — so the long-poll probe said "there is work"
#     the whole time, which is why consumers kept polling and kept getting
#     empty;
#   * the broker serves pops from an in-memory hot-list ring
#     (server/src/hotlist.rs, queen.log_pop_list_v1) that REPLACES the SQL
#     candidate scan, and a pop that finds a candidate empty CAS-clears it from
#     the ring;
#   * config.rs:683  hotlist_reseed_ms = env_int("QUEEN_HOTLIST_RESEED_MS", 30000)
#     — the ring is re-seeded from SQL only every 30s, and the measured dark
#     periods end at 29997 / 30015 ms.
#
# So: a partition dropped from the ring is invisible until the next reseed,
# bounded by 30s, with the data durably in the log the whole time.
#
# QUEEN_HOTLIST=0 reverts to the legacy per-pop SQL candidate scan. Same load,
# same cell, ring on vs ring off. If the stall only happens with the ring on,
# the mechanism is proven rather than argued.
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=$1; RATE=$2
DROPIN=/run/systemd/system/queen-broker.service.d
mkdir -p "$OUT"

run() { # runid hotlist-value
  local id=$1 hl=$2
  mkdir -p $DROPIN
  printf '[Service]\nEnvironment=QUEEN_HOTLIST=%s\n' "$hl" >$DROPIN/hotlist.conf
  systemctl daemon-reload
  bash $G/reset-cell-db.sh >/dev/null 2>&1   # restarts the broker -> picks it up
  local got
  got=$(tr '\0' '\n' </proc/$(systemctl show queen-broker -p MainPID --value)/environ | grep '^QUEEN_HOTLIST=' || echo "QUEEN_HOTLIST=<unset>")
  echo "  [$id] broker env: $got"
  bash $G/runpt.sh "$OUT" "$id" -- \
    -mode cloud -tenants-file "$OUT/tenants.json" -tenants 4 -shared-queue \
    -queue orders -group workers -partitions 4 -push-batch 1 \
    -producers-per-tenant 2 -consumers-per-tenant 4 -pop-batch 50 -pop-wait \
    -payload 256 -target broker -rate "$RATE" -duration 60 -drain 60 \
    -fail-on-verify=false -out "$OUT" -run-id "$id" \
    -note "HOTLIST A/B: QUEEN_HOTLIST=$hl, rate=$RATE, direct to broker, $(cat /sys/fs/cgroup/queencell.slice/cpu.max) cpu.max" \
    >/dev/null 2>&1
  echo "  [$(date -u +%T)] $id done"
}

# interleaved, twice each: the stall is stochastic, so one clean run proves nothing
run hl-on-1  1
run hl-off-1 0
run hl-on-2  1
run hl-off-2 0

rm -f $DROPIN/hotlist.conf; systemctl daemon-reload
bash $G/reset-cell-db.sh >/dev/null 2>&1
echo "[hotlist-ab] done, drop-in removed, broker back to defaults"
