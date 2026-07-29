#!/usr/bin/env bash
# stall-diag2.sh <outdir> <run-id> <rate> [target]
#
# Round 2. Round 1 killed the lease hypothesis: through a 29s stall the
# partition's consumer row had worker_id NULL and lease_expires_at NULL while
# 2616 messages sat past `committed`. So the lease filter in the wildcard
# candidate set (043_log_pop.sql:568-569) is NOT what hid it.
#
# That leaves exactly one other term in the candidate filter (:566):
#
#     AND p.last_write_at >= v_watermark - interval '2 minutes'
#
# where v_watermark is queen.consumer_watermarks.last_empty_scan_at for
# (tenant, queue, group) — advanced to now() by a scan that found nothing
# claimable, and re-verified at most once every 30 seconds (:608).
#
# So this samples, per second and per partition with a backlog: last_write_at,
# the group's last_empty_scan_at, and the SIGN of the filter term. If
# `passes_filter` is false while a backlog exists, the empty-scan floor is the
# mechanism and the 2-minute slack is not covering whatever staleness
# last_write_at has.
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=$1; RUNID=$2; RATE=$3; TARGET=${4:-broker}
mkdir -p "$OUT"
export PGPASSWORD=postgres
CELLIP=$(cat /root/cell/cellpg.ip)
PSQL=(psql -h "$CELLIP" -p 5432 -U postgres -d queen -qtAX)

bash $G/reset-cell-db.sh >/dev/null 2>&1

(
  echo "t_unix,tenant,part,backlog,lease_ms_left,worker,last_write_age_s,empty_scan_age_s,passes_write_filter,has_pending"
  while :; do
    now=$(date +%s.%N)
    "${PSQL[@]}" -F',' -c "
      SELECT left(q.tenant_id::text,8), p.name,
             p.last_offset - coalesce(c.committed,-1),
             round(extract(epoch from (c.lease_expires_at - clock_timestamp()))*1000),
             coalesce(left(c.worker_id,10),'-'),
             round(extract(epoch from (clock_timestamp() - p.last_write_at))::numeric,1),
             round(extract(epoch from (clock_timestamp() - w.last_empty_scan_at))::numeric,1),
             (p.last_write_at >= coalesce(w.last_empty_scan_at,'1970-01-01'::timestamptz) - interval '2 minutes'),
             queen.log_has_pending_v1(q.tenant_id, q.name, 'workers')
        FROM queen.log_partitions p
        JOIN queen.log_queues q ON q.id = p.queue_id
        LEFT JOIN queen.log_consumers c
               ON c.partition_id = p.id AND c.consumer_group = 'workers'
        LEFT JOIN queen.consumer_watermarks w
               ON w.tenant_id = q.tenant_id AND w.queue_name = q.name
              AND w.consumer_group = 'workers'
       WHERE p.last_offset - coalesce(c.committed,-1) > 200" 2>>"$OUT/$RUNID.diag2.err" |
    while IFS= read -r l; do [ -n "$l" ] && echo "$now,$l"; done
    sleep 1
  done
) >"$OUT/$RUNID.stalldiag2.csv" &
DIAG=$!

bash $G/runpt.sh "$OUT" "$RUNID" -- \
  -mode cloud -tenants-file "$OUT/tenants.json" -tenants 4 -shared-queue \
  -queue orders -group workers -partitions 4 -push-batch 1 \
  -producers-per-tenant 2 -consumers-per-tenant 4 -pop-batch 50 -pop-wait \
  -payload 256 -target "$TARGET" -rate "$RATE" -duration 45 -drain 90 \
  -fail-on-verify=false -out "$OUT" -run-id "$RUNID" \
  -note "STALL DIAG 2 rate=$RATE target=$TARGET; watermark/last_write_at instrumented" \
  >/dev/null 2>&1

kill $DIAG 2>/dev/null; wait $DIAG 2>/dev/null
echo "[stall-diag2] $RUNID -> $OUT/$RUNID.stalldiag2.csv"
head -3 "$OUT/$RUNID.diag2.err" 2>/dev/null
