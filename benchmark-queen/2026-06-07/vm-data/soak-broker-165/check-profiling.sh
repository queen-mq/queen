#!/bin/bash
PSQL(){ docker exec postgres psql -U postgres -d postgres -tAc "$1" 2>/dev/null; }
echo "=== my external sampler (long-mon) running? ==="
pgrep -af "long-mon" | grep -v grep | head -2
echo "long-mon2.log samples: $(grep -c xc_ins /root/bench-runs/long-mon2.log 2>/dev/null)  first/last:"
grep xc_ins /root/bench-runs/long-mon2.log 2>/dev/null | sed -n '1p;$p'
echo
echo "=== broker built-in queen.worker_metrics (columns) ==="
PSQL "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema='queen' AND table_name='worker_metrics'"
echo "--- worker_metrics span + count ---"
PSQL "SELECT count(*)||' rows, '||min(created_at)||' -> '||max(created_at) FROM queen.worker_metrics"
echo "--- worker_metrics recent sample ---"
PSQL "SELECT * FROM queen.worker_metrics ORDER BY created_at DESC LIMIT 1"
echo
echo "=== worker_metrics_summary (columns) ==="
PSQL "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema='queen' AND table_name='worker_metrics_summary'"
echo
echo "=== /metrics (prometheus) endpoint ==="
curl -s --max-time 5 -o /dev/null -w "GET /metrics -> HTTP %{http_code} in %{time_total}s\n" http://localhost:6632/metrics
curl -s --max-time 5 http://localhost:6632/metrics 2>/dev/null | grep -iE "push|pop|ack|delet|throughput|cpu|rss|messages" | head -20
