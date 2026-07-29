#!/usr/bin/env bash
# The "bench" plan: limits far above anything the cell can serve, so a ceiling
# run measures the BROKER and not the proxy's token bucket. Re-run after every
# `vm-cell.sh up` — up() recreates pxdb from scratch and the migrations only
# seed free/dev/pro/dedicated-s.
set -euo pipefail
docker exec -i cell-pxdb psql -qtA -v ON_ERROR_STOP=1 -U postgres -d queen_proxy <<'SQL'
INSERT INTO queen_proxy.plans
  (code, cell_class, max_req_per_sec, req_burst, max_msgs_per_sec, msgs_burst,
   max_queues, max_partitions_per_queue, max_parked_pops, max_payload_bytes,
   max_batch_items, max_retained_bytes, max_retention_seconds, monthly_msgs_quota)
VALUES
  ('bench','shared', 2000000, 4000000, 2000000, 4000000,
   10000, 256, 200000, 4194304,
   100000, 1099511627776, 2592000, 1000000000000)
ON CONFLICT (code) DO UPDATE SET
  max_req_per_sec = EXCLUDED.max_req_per_sec,
  req_burst = EXCLUDED.req_burst,
  max_msgs_per_sec = EXCLUDED.max_msgs_per_sec,
  msgs_burst = EXCLUDED.msgs_burst,
  max_queues = EXCLUDED.max_queues,
  max_partitions_per_queue = EXCLUDED.max_partitions_per_queue,
  max_parked_pops = EXCLUDED.max_parked_pops,
  max_payload_bytes = EXCLUDED.max_payload_bytes,
  max_batch_items = EXCLUDED.max_batch_items,
  max_retained_bytes = EXCLUDED.max_retained_bytes,
  max_retention_seconds = EXCLUDED.max_retention_seconds,
  monthly_msgs_quota = EXCLUDED.monthly_msgs_quota;
SELECT code, max_req_per_sec, max_msgs_per_sec, max_parked_pops FROM queen_proxy.plans ORDER BY code;
SQL
