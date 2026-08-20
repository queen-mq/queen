#!/bin/bash
# Clean slate between scenarios: drop the queen DB, restart the broker so it
# re-applies the schema from scratch. Each July scenario started from its own
# DB state, so a rerun that inherits the previous scenario's queues is not the
# same measurement.
set -uo pipefail
ssh -o BatchMode=yes root@46.101.193.166 '
docker stop queen >/dev/null 2>&1
psql -h 127.0.0.1 -U postgres -d postgres -qc "DROP DATABASE IF EXISTS queen WITH (FORCE);" >/dev/null
psql -h 127.0.0.1 -U postgres -d postgres -qc "CREATE DATABASE queen;" >/dev/null
psql -h 127.0.0.1 -U postgres -d queen -qc "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;" >/dev/null
docker start queen >/dev/null
for i in $(seq 30); do
  curl -sf http://127.0.0.1:6632/health >/dev/null 2>&1 && break
  sleep 1
done
curl -s http://127.0.0.1:6632/health; echo
psql -h 127.0.0.1 -U postgres -d queen -qtAc "SELECT count(*) || \" queues\" FROM queen.queues"
'
