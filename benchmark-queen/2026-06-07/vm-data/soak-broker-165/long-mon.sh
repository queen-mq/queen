#!/bin/bash
# BROKER: long-run monitor. Every 30s: CPU + PG commits/inserts/completed + table size + dead/live.
START_TIME=$(date -u +%FT%T.000Z)
echo "monitor_start=$START_TIME"
while true; do
  TS=$(date -u +%FT%TZ)
  CPU=$(docker stats --no-stream --format "{{.Name}}={{.CPUPerc}}/{{.MemUsage}}" queen postgres 2>/dev/null | tr "\n" " ")
  ROW=$(docker exec postgres psql -U postgres -d postgres -tAc \
    "SELECT (SELECT xact_commit FROM pg_stat_database WHERE datname='postgres'),
            COALESCE((SELECT n_tup_ins FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0),
            COALESCE((SELECT n_tup_del FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0),
            pg_size_pretty(pg_total_relation_size('queen.messages')),
            COALESCE((SELECT n_dead_tup FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0),
            COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0)" 2>/dev/null | tr "|" " ")
  DF=$(df -BG / | tail -1 | awk '{print $4" free"}')
  echo "$TS | $CPU | xc_ins_del_size_dead_live= $ROW | disk=$DF"
  sleep 30
done
