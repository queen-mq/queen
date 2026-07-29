#!/usr/bin/env bash
# b3-toplevel.sh <outdir> <run-id> <rate> — commit attribution done properly.
#
# The first B3 pass dumped pg_stat_statements with track=all, which records the
# statements NESTED INSIDE the stored procedures as well. Those share their
# caller's transaction, so their `calls` are not commits and the per-bucket
# calls/msg came out at 11.6 for "other" — an artefact of counting the inside of
# a procedure as if it were a transaction.
#
# A commit is a TOP-LEVEL statement. pg_stat_statements.toplevel says which is
# which, so this run dumps it and the analysis sums only toplevel=true. The
# total should reconcile with pg_stat_database.xact_commit over the same window,
# which is what makes the breakdown trustworthy rather than merely plausible.
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=$1; RUNID=$2; RATE=$3
export PGPASSWORD=postgres

bash $G/reset-cell-db.sh >/dev/null 2>&1
CELLIP=$(cat /root/cell/cellpg.ip)
PSQL=(psql -h "$CELLIP" -p 5432 -U postgres -d queen -qtAX)

"${PSQL[@]}" -c "SELECT pg_stat_statements_reset()" >/dev/null 2>&1

bash $G/runpt.sh "$OUT" "$RUNID" -- \
  -mode cloud -tenants-file "$OUT/tenants.json" -tenants 4 -shared-queue \
  -queue orders -group workers -partitions 4 -push-batch 1 \
  -producers-per-tenant 2 -consumers-per-tenant 4 -pop-batch 50 -pop-wait \
  -payload 256 -target proxy -rate "$RATE" -duration 60 -drain 45 \
  -fail-on-verify=false -out "$OUT" -run-id "$RUNID" \
  -note "B3 commit attribution, toplevel-aware pg_stat_statements; cell 2c/8G PGSS=1; through proxy" \
  >/dev/null 2>&1

"${PSQL[@]}" -F',' -c "
  SELECT toplevel, queryid, calls, rows, round(total_exec_time::numeric,1),
         replace(left(query,150), ',', ';')
    FROM pg_stat_statements
   WHERE dbid=(SELECT oid FROM pg_database WHERE datname='queen')
   ORDER BY toplevel DESC, calls DESC" >"$OUT/$RUNID.pgss-toplevel.csv"

echo "[b3-toplevel] $RUNID -> $OUT/$RUNID.pgss-toplevel.csv ($(wc -l <"$OUT/$RUNID.pgss-toplevel.csv") rows)"
