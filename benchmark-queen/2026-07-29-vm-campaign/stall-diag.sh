#!/usr/bin/env bash
# stall-diag.sh <outdir> <run-id> <rate> <lease-seconds>
#
# Reproduce the partition delivery stall and OBSERVE it instead of inferring it.
#
# 043_log_pop.sql:558-569 builds the wildcard candidate set from partitions that
# are (a) written recently, (b) have last_offset > committed, and (c) are
# lease-free. A partition failing only (c) is invisible to every consumer of the
# group until the lease expires. This samples, once a second, every partition
# with a backlog together with the state of its consumer row, so a stall can be
# read off directly: backlog > 0 AND a live lease => (c); backlog > 0 AND no
# lease => something else, and the lease theory is dead.
#
# Run it at two lease times: if the stall duration tracks leaseTime, that IS the
# mechanism.
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=$1; RUNID=$2; RATE=$3; LEASE=$4
mkdir -p "$OUT"
export PGPASSWORD=postgres
CELLIP=$(cat /root/cell/cellpg.ip)
PSQL=(psql -h "$CELLIP" -p 5432 -U postgres -d queen -qtAX)

bash $G/reset-cell-db.sh >/dev/null 2>&1

(
  echo "t_unix,tenant,partition,last_offset,committed,backlog,lease_ms_left,worker_id"
  while :; do
    now=$(date +%s.%N)
    "${PSQL[@]}" -F',' -c "
      SELECT left(q.tenant_id::text,8), p.name, p.last_offset, c.committed,
             p.last_offset - coalesce(c.committed,-1) AS backlog,
             round(extract(epoch from (c.lease_expires_at - clock_timestamp()))*1000) AS lease_ms_left,
             coalesce(left(c.worker_id,12),'-')
        FROM queen.log_partitions p
        JOIN queen.log_queues q ON q.id = p.queue_id
        LEFT JOIN queen.log_consumers c
               ON c.partition_id = p.id AND c.consumer_group = 'workers'
       WHERE p.last_offset - coalesce(c.committed,-1) > 200" 2>/dev/null |
    while IFS= read -r l; do [ -n "$l" ] && echo "$now,$l"; done
    sleep 1
  done
) >"$OUT/$RUNID.stalldiag.csv" &
DIAG=$!

bash $G/runpt.sh "$OUT" "$RUNID" -- \
  -mode cloud -tenants-file "$OUT/tenants.json" -tenants 4 -shared-queue \
  -queue orders -group workers -partitions 4 -push-batch 1 \
  -producers-per-tenant 2 -consumers-per-tenant 4 -pop-batch 50 -pop-wait \
  -payload 256 -target broker -rate "$RATE" -duration 45 -drain 120 \
  -lease-time "$LEASE" -fail-on-verify=false -out "$OUT" -run-id "$RUNID" \
  -note "STALL DIAG rate=$RATE leaseTime=${LEASE}s; cell 4c/8G slice-capped; direct to broker" \
  >/dev/null 2>&1

kill $DIAG 2>/dev/null; wait $DIAG 2>/dev/null
echo "[stall-diag] $RUNID lease=${LEASE}s rate=$RATE -> $OUT/$RUNID.stalldiag.csv"
