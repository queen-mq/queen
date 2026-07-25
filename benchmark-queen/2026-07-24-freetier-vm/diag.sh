#!/usr/bin/env bash
# diag.sh — pin the ~490/s ceiling: raw disk fsync rate + PG wait events under a
# 500/s load + commit throughput. Decides fsync-bound vs lock-bound vs CPU-bound.
set -uo pipefail
PG=qbench-pg; GL=/root/bench/goload-linux-amd64-fresh
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }
echo "=== raw disk fdatasync microbench (pg_test_fsync, 3s/method) ==="
docker exec -u postgres "$PG" pg_test_fsync -f /tmp/fsynctest -s 3 2>/dev/null | \
  grep -iE "fdatasync|open_datasync|fsync " | grep ops | head -6 || echo "  (pg_test_fsync unavailable)"
rm -f /tmp/fsynctest 2>/dev/null; docker exec "$PG" rm -f /tmp/fsynctest 2>/dev/null || true

C0=$(q "select xact_commit from pg_stat_database where datname='postgres';")
"$GL" -mode tenants -url http://127.0.0.1:6682 -tenants 10 -queues-per-tenant 10 \
  -phase-high 50 -phase-low 50 -phase-sec 999 -duration 45 -report 40 >/root/bench/out/diag.out 2>&1 &
P=$!
sleep 8
echo "=== PG active wait events during ~500/s load (5 samples @5s) ==="
for i in 1 2 3 4 5; do
  q "select coalesce(wait_event_type,'CPU')||':'||coalesce(wait_event,'run') w, count(*) c from pg_stat_activity where state='active' and pid<>pg_backend_pid() group by 1 order by 2 desc limit 5" \
    | tr '\n' '  '; echo
  sleep 5
done
wait $P 2>/dev/null
C1=$(q "select xact_commit from pg_stat_database where datname='postgres';")
awk -v a="$C0" -v b="$C1" 'BEGIN{printf "=== commits/s over load window = %.0f ===\n",(b-a)/45.0}'
echo "--- goload diag tail ---"; tail -3 /root/bench/out/diag.out
